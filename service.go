package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"sync"
	"time"
	"sync/atomic"
)


type AdminServerManager struct {
	*Manager
}

func NewAdminServerManager(manager *Manager) (*AdminServerManager) {
	return &AdminServerManager{Manager: manager}
}

func (as *AdminServerManager) Logging(in *Nothing, stream Admin_LoggingServer) error {
	for {
		ti := atomic.LoadInt32(&as.count)
		for {
			if ti < atomic.LoadInt32(&as.count) {
				break
			}
		}
		as.mu.Lock()
		if err := stream.Send(as.Event); err != nil {
			return err
		}
		as.mu.Unlock()
	}
	return nil
}

func (as *AdminServerManager) Statistics(in *StatInterval, stream Admin_StatisticsServer) error {
	m := NewStat()
	c := time.Tick(time.Second * time.Duration(in.IntervalSeconds))
	ti := atomic.LoadInt32(&as.count)
	for {
		select {
		case <-c:
			if err := stream.Send(&m); err != nil {
				return err
			}
			m = NewStat()
			break
		default:
			if ti < atomic.LoadInt32(&as.count) {
				ti = atomic.LoadInt32(&as.count)
				m.ByMethod[as.Event.Method]++
				m.ByConsumer[as.Event.Consumer]++
			}
		}
	}
	return nil
}

func (as *AdminServerManager) AdminStreamInterceptor1(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (error) {
	md, _ := metadata.FromIncomingContext(ss.Context())
	if _, ok := md["consumer"]; !ok {
		return status.Error(codes.Unauthenticated, "Unauthenticated")
	}
	consumer := md["consumer"][0]
	err := as.AuthLogin(consumer, info.FullMethod)
	if grpc.Code(err) != codes.OK {
		return err
	}

	as.ChangeEvent(&Event{Timestamp: time.Now().UnixNano(), Consumer: consumer, Method: info.FullMethod, Host: "127.0.0.1:"})
	atomic.AddInt32(&as.count,1)
	err = handler(srv, ss)

	return err
}

type BizServerManager struct {
	*Manager
}

func NewBizServer(manager *Manager) *BizServerManager {
	return &BizServerManager{Manager: manager}
}

func (bz *BizServerManager) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bz *BizServerManager) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bz *BizServerManager) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bz *BizServerManager) BizUnaryInterceptor1(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler, ) (interface{}, error) {

	md, _ := metadata.FromIncomingContext(ctx)
	if _, ok := md["consumer"]; !ok {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}
	consumer := md["consumer"][0]
	err := bz.AuthLogin(consumer, info.FullMethod)

	if grpc.Code(err) != codes.OK {
		return nil, err
	}

	bz.ChangeEvent(&Event{Timestamp: time.Now().UnixNano(), Consumer: consumer, Method: info.FullMethod, Host: "127.0.0.1:"})
	atomic.AddInt32(&bz.count,1)
	reply, err := handler(ctx, req)

	return reply, err
}

func NewStat() Stat {
	return Stat{ByConsumer: map[string]uint64{}, ByMethod: map[string]uint64{}}
}

type Manager struct {
	Event      *Event
	ACLData    map[string][]string
	mu         *sync.Mutex
	count      int32
}

func NewManager(acldata string) (*Manager, error) {
	adata := make(map[string][]string)
	err := json.Unmarshal([]byte(acldata), &adata)
	if err != nil {
		return nil, err
	}
	m := &sync.Mutex{}

	return &Manager{ACLData: adata, mu: m}, nil
}

func (m *Manager) ChangeEvent(event *Event) {
	m.mu.Lock()
	m.Event = event
	m.mu.Unlock()
}

func (m Manager) AuthLogin(consumer string, method string) error {
	if _, ok := m.ACLData[consumer]; ok {
		methods, ok := m.ACLData[consumer]
		if !ok {
			return fmt.Errorf("Argument is not a slice")
		}

		for _, val := range methods {
			v := strings.TrimRight(val, "*")
			if strings.Contains(method, v) {
				return status.Error(codes.OK, "authenticated ok")
			}
		}

	}
	return status.Error(codes.Unauthenticated, "Unauthenticated")
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {

	manager, err := NewManager(ACLData)
	if err != nil {
		return err
	}

	bizServer := NewBizServer(manager)
	adminServer := NewAdminServerManager(manager)

	go func() {
		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalln("cant listet port", err)
		}
		server := grpc.NewServer(grpc.UnaryInterceptor(bizServer.BizUnaryInterceptor1), grpc.StreamInterceptor(adminServer.AdminStreamInterceptor1))
		RegisterBizServer(server, bizServer)
		RegisterAdminServer(server, adminServer)
		go server.Serve(lis)
		//fmt.Println("starting server at " + listenAddr)

		defer server.Stop()
		defer lis.Close()

		select {
		case <-ctx.Done():
			server.Stop()
		}
	}()
	return nil
}

