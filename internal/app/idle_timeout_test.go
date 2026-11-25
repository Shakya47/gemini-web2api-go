package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

// 上游开流后长时间不来 chunk（#81 实测：首字节 2 秒就到、之后 200 多秒静默）：
// 空闲看门狗要在 idle 内掐断、报 errIdleTimeout，而不是等到整体超时。
func TestReadBodyWithIdle_Fires(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("frame1\n"))
		w.(http.Flusher).Flush()
		// 开流后长时间静默；看门狗掐断客户端连接后 r.Context() 取消，这里立即退出，
		// 免得 srv.Close() 干等满 5 秒拖慢整套测试。
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var lines int
	start := time.Now()
	raw, _, err := readBodyWithIdle(resp.Body, func(string) error { lines++; return nil }, start, 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != errIdleTimeout {
		t.Fatalf("空闲超时没触发，拿到的是 %T %v", err, err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("掐断太慢：%v（应在 ~200ms 空闲后触发，而不是等满 5 秒）", elapsed)
	}
	if lines != 1 {
		t.Errorf("应只收到首帧 1 行，实际 %d", lines)
	}
	if !strings.Contains(string(raw), "frame1") {
		t.Errorf("partial raw 丢了首帧：%q", raw)
	}
}

// 帧间隔小于 idle 时不该误触发；流正常结束要拿到全部内容、不报错。
func TestReadBodyWithIdle_NoFalseTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "frame%d\n", i)
			fl.Flush()
			time.Sleep(60 * time.Millisecond) // < idle，每帧都刷新计时
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var lines int
	raw, _, err := readBodyWithIdle(resp.Body, func(string) error { lines++; return nil }, time.Now(), 300*time.Millisecond)
	if err != nil {
		t.Fatalf("持续出帧不该出错：%v", err)
	}
	if lines != 5 {
		t.Errorf("应收到 5 行，实际 %d", lines)
	}
	if !strings.Contains(string(raw), "frame4") {
		t.Errorf("末帧丢了：%q", raw)
	}
}

// idle<=0 时退回普通 readBody，装不装看门狗对正常响应结果一致。
func TestReadBodyWithIdle_Disabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("a\nb\nc\n"))
	}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _, err := readBodyWithIdle(resp.Body, nil, time.Now(), 0)
	if err != nil {
		t.Fatalf("idle=0 不该出错：%v", err)
	}
	if string(raw) != "a\nb\nc\n" {
		t.Errorf("raw 不对：%q", raw)
	}
}

// 空闲超时的重试语义：不能被当成"上游总超时"，否则 retry_on_timeout=关 时会连它一起挡掉。
// 它是瞬时抖动，必须无条件重试。
func TestIdleTimeoutRetrySemantics(t *testing.T) {
	if isUpstreamTimeout(errIdleTimeout) {
		t.Error("errIdleTimeout 不该算 isUpstreamTimeout（会被 retry_on_timeout=关 一起挡掉）")
	}
	if !retryAfterTimeout(errIdleTimeout) {
		t.Error("errIdleTimeout 应当总是重试")
	}
}

func TestReadBodyWithIdle_BlankKeepaliveDoesNotReset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		w.Write([]byte("frame1\n"))
		fl.Flush()
		for i := 0; i < 8; i++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(60 * time.Millisecond):
				w.Write([]byte("\n"))
				fl.Flush()
			}
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	start := time.Now()
	_, _, err = readBodyWithIdle(resp.Body, nil, start, 200*time.Millisecond)
	elapsed := time.Since(start)
	if err != errIdleTimeout {
		t.Fatalf("expected errIdleTimeout despite blank keepalive lines, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("watchdog fired too slowly (%v)", elapsed)
	}
}

func TestReadBodyWithIdle_TLSClientHTTP2(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		w.Write([]byte("frame1\n"))
		fl.Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()

	tc, err := tls_client.NewHttpClient(
		tls_client.NewNoopLogger(),
		tls_client.WithTimeoutSeconds(5),
		tls_client.WithClientProfile(profiles.Chrome_146),
		tls_client.WithInsecureSkipVerify(),
	)
	if err != nil {
		t.Fatal(err)
	}
	req, err := fhttp.NewRequest("POST", srv.URL, strings.NewReader("x=1"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	start := time.Now()
	_, _, err = readBodyWithIdle(resp.Body, nil, start, 200*time.Millisecond)
	elapsed := time.Since(start)
	if err != errIdleTimeout {
		t.Fatalf("expected errIdleTimeout over tls-client H2, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("tls-client idle watchdog took too long (%v)", elapsed)
	}
}

func TestDoGeminiRequestCtx_ClientCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	// Route through proxy branch of doGeminiRequestCtx using a no-op local server check
	// via canceled ctx before request is sent.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, err := doGeminiRequestCtx(ctx, srv.URL, "f.req=1", nil, srv.URL, nil)
	if err == nil {
		t.Fatal("expected error when caller ctx is canceled")
	}
}
