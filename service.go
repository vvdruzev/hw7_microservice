package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type AdminServerManager struct {
	*Manager
	*ManagerStat
}

func NewAdminServerManager(manager *Manager) (*AdminServerManager) {
	return &AdminServerManager{Manager:manager}
}

func (as *AdminServerManager) Logging(in *Nothing, stream Admin_LoggingServer) error {

	md, _ := metadata.FromIncomingContext(stream.Context())
	consumer := md["consumer"][0]
	t := stream.Context().Value("method").(string)
	as.ChangeEvent(&Event{Timestamp:time.Now().UnixNano(),Consumer: consumer, Method: t, Host: "127.0.0.1:"})
	if !as.loggerFlag {
		if err := stream.Send(as.Event); err != nil {
			return err
		}
		as.loggerFlag = true
	}
	for {
		ti := as.count
		time.Sleep(1 * time.Millisecond)
		for {
			if ti < as.count {
				break
			}
		}
		if err := stream.Send(as.Event); err != nil {
			fmt.Println("sdfsdfdf")
			return err
		}
	}
	return nil
}

func NewStat() Stat  {
	return Stat{ByConsumer: map[string]uint64{},ByMethod: map[string]uint64{}}
}


func (as *AdminServerManager) Statistics(in *StatInterval, stream Admin_StatisticsServer) error {
	//timer:=time.NewTimer(time.Second*time.Duration(in.IntervalSeconds))
	//as.MsStat = *NewManagerStat()
//	as.MsStat.ByConsumer = make(map[string]uint64)
fmt.Println("-------",as.Stat)
	start := time.Now()
	fmt.Println(as.statFlag)
	if !as.statFlag {
		//as.Stat = Stat{ByConsumer: map[string]uint64{},ByMethod: map[string]uint64{}}
		md, _ := metadata.FromIncomingContext(stream.Context())
		consumer := md["consumer"][0]
		method := stream.Context().Value("method").(string)
		as.CounterMethod(method)
		as.CounterConsumer(consumer)
		as.statFlag  =true
	}
	fmt.Println(as.statFlag)

	fmt.Println(as.Stat)
	for  {
		time.Sleep(time.Second*time.Duration(in.IntervalSeconds))

		if err := stream.Send(&as.Stat); err != nil {
			return err
		}
		fmt.Println(as.Stat)

		//as.mu.Lock()
		//as.Stat = &Stat{ByConsumer: map[string]uint64{},ByMethod: map[string]uint64{}}
		//as.mu.Unlock()
		fmt.Println(time.Since(start))
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
	as.ManagerStat = NewManagerStat()
	//as.Stat = &Stat{ByConsumer: map[string]uint64{},ByMethod:map[string]uint64{}}//NewStat()
	//as.ByMethod = map[string]uint64{}

	err = handler(srv, ss)

	return err
}

type BizServerManager struct {
	*Manager
	*ManagerStat
}

func NewBizServer(manager *Manager) *BizServerManager {
	return &BizServerManager{Manager:manager}
}

func (bz *BizServerManager) Check(ctx context.Context, in *Nothing) (*Nothing, error) {

	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md["consumer"][0]
	t := ctx.Value("method").(string)

	bz.ChangeEvent(&Event{Timestamp:time.Now().UnixNano(),Consumer: consumer, Method: t, Host: "127.0.0.1:"})
	bz.CountIncrement()

	return &Nothing{}, nil
}
func (bz *BizServerManager) Add(ctx context.Context, in *Nothing) (*Nothing, error) {

	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md["consumer"][0]
	t := ctx.Value("method").(string)

	bz.ChangeEvent(&Event{Timestamp:time.Now().UnixNano(),Consumer: consumer, Method: t, Host: "127.0.0.1:"})
	bz.CountIncrement()

	return &Nothing{}, nil
}

func (bz *BizServerManager) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md["consumer"][0]
	t := ctx.Value("method").(string)

	bz.ChangeEvent(&Event{Timestamp:time.Now().UnixNano(),Consumer: consumer, Method: t, Host: "127.0.0.1:"})
	bz.CountIncrement()

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

	bz.CounterMethod(info.FullMethod)
	bz.CounterConsumer(consumer)


	reply, err := handler(ctx, req)

	return reply, err
}

type ManagerStat struct {
	Stat
	mu *sync.Mutex
}

func NewManagerStat() *ManagerStat  {
	m:= make(map[string]uint64)
	c:= make(map[string]uint64)
	return &ManagerStat{Stat:Stat{ByMethod: m,ByConsumer:c}}
}
func (m *ManagerStat) CounterMethod(method string) {
	//m.mu.Lock()
	fmt.Println(method)

	m.Stat.ByMethod[method]++
	//m.mu.Unlock()
}

func (m *ManagerStat) CounterConsumer(consumer string) {
//	m.mu.Lock()
	fmt.Println(consumer)

	m.Stat.ByConsumer[consumer]++
//	m.mu.Unlock()

}


type Manager struct {
	Event *Event
	ACLData map[string]interface{}
	mu *sync.Mutex
	count int
	loggerFlag bool
	//Stat *Stat
	statFlag bool
}

func NewManager(acldata string) (*Manager, error) {
	adata := make(map[string]interface{})
	err := json.Unmarshal([]byte(acldata), &adata)
	if err != nil {
		return nil, err
	}
	m := &sync.Mutex{}
	//m:= make(map[string]int)
	//b:= make(map[string]int)

	return &Manager{ACLData: adata,loggerFlag:false, statFlag:false,mu:m}, nil
}

func (m *Manager) CountIncrement()  {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
}

func (m *Manager) ChangeEvent(event *Event)  {
	m.mu.Lock()
	m.Event = event
	m.mu.Unlock()
}
func (m *Manager) CheckEvent() bool  {
	return false
}



func (m Manager) AuthLogin(consumer string, method string) error {
	if _, ok := m.ACLData[consumer]; ok {
		methods, ok := m.ACLData[consumer].([]interface{})
		if !ok {
			return fmt.Errorf("Argument is not a slice")
		}

		for _, val := range methods {
			v := strings.TrimRight(val.(string), "*")
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
		server := grpc.NewServer(grpc.UnaryInterceptor(bizServer.BizUnaryInterceptor1), grpc.InTapHandle(rateLimiter), grpc.StreamInterceptor(adminServer.AdminStreamInterceptor1))
		RegisterBizServer(server, bizServer)
		RegisterAdminServer(server, adminServer)
		go server.Serve(lis)
		fmt.Println("starting server at " + listenAddr)

		defer server.Stop()
		defer lis.Close()

		select {
		case <-ctx.Done():
			server.Stop()
		}
	}()
	return nil
}

// -----

func rateLimiter(ctx context.Context, info *tap.Info) (context.Context, error) {
	ctx = context.WithValue(ctx, "method", info.FullMethodName)

	return ctx, nil
}

