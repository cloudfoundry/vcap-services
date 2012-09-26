package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"proxy_tunnel/logger"
	"testing"
	"time"
)

var data = "1234567890"
var errCha = make(chan error, 1)
var dataCha = make(chan []byte, 1)

func initTunnel(ePort uint, iPort uint) (runTunnel *Tunnel) {
	runTunnel = &Tunnel{EPort: ePort, IPort: iPort, BlockSize: 65536, IIp: net.ParseIP("127.0.0.1")}
	return
}

func startTestSvr(port int) {
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			errCha <- err
			return
		}
		conn, err := ln.Accept()
		if err != nil {
			errCha <- err
			return
		}
		buf := make([]byte, 2048, 2048)
		for {
			num, err := conn.Read(buf)
			if err != nil && err != io.EOF {
				errCha <- err
				return
			}
			switch {
			case num > 0:
				dataCha <- buf[:num]
			case num == 0 || err == io.EOF:
				dataCha <- []byte("close")
				return
			default:
				errCha <- errors.New("Invalid Read Return")
			}
		}
	}()
}

func startSvr(tu *Tunnel) {
	go func() {
		logger.Init("")
		defer logger.Finalize()
		Run(tu)
	}()
}

func TestRun(t *testing.T) {
	tu := initTunnel(64003, 64004)
	startSvr(tu)
	startWait := 3
	select {
	case err := <-errCha:
		t.Errorf("Start Running Server Error [%s]", err)
	default:
		time.Sleep(1 * time.Second)
		startWait--
		if startWait <= 0 {
			t.Log("Pass Case [Run]")
			time.Sleep(3 * time.Second)
			return
		}
	}
}

func TestPass(t *testing.T) {
	tu := initTunnel(64001, 64002)
	startTestSvr(64002)
	startSvr(tu)
	time.Sleep(1 * time.Second)
	conn, err := net.Dial("tcp", "127.0.0.1:64001")
	if err != nil {
		t.Errorf("Connect External Port 64001 Error [%s]", err)
	}
	conn.Write([]byte(data))
	for i := 0; i < 3; {
		select {
		case err = <-errCha:
			t.Errorf("Start Running Server Error [%s]", err)
		case recvData := <-dataCha:
			if string(recvData) != data {
				t.Errorf("Recv [%s] Not Equal To [%s]", string(recvData), data)
			}
			t.Log("Pass Case [Pass]")
			return
		default:
			time.Sleep(1 * time.Second)
			i++
		}
	}
	t.Error("Recv Data Timeout")
}

func TestBlock(t *testing.T) {
	tu := initTunnel(64005, 64006)
	tu.BlockSize = 2000
	startTestSvr(64006)
	startSvr(tu)
	time.Sleep(1 * time.Second)
	conn, err := net.Dial("tcp", "127.0.0.1:64005")
	if err != nil {
		t.Errorf("Connect External Port 64005 Error [%s]", err)
	}
	headData := make([]byte, 2000, 2000)
	conn.Write(headData)
	checkHead := false
	for i := 0; i < 3; {
		select {
		case err = <-errCha:
			t.Errorf("Start Running Server Error [%s]", err)
		case recvData := <-dataCha:
			if !checkHead && len(recvData) != 2000 {
				t.Errorf("Recv Head Size [%d] Not [%d]", len(recvData), 2000)
			} else if checkHead == false {
				checkHead = true
				conn.Write([]byte(data))
			}
			if string(recvData) == "close" {
				t.Log("Pass Case [Block]")
				return
			}
		default:
			time.Sleep(1 * time.Second)
			i++
		}
	}
}
