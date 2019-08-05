package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"log"
	"net"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type AdminServerManager struct {
	savedEvent []*Event
	ACLData    map[string]interface{}
}

func NewAdminServerManager(acldata string) (*AdminServerManager, error) {
	adata := make(map[string]interface{})
	err := json.Unmarshal([]byte(acldata), &adata)
	if err != nil {
		return nil, err
	}
	return &AdminServerManager{ACLData: adata}, nil
}

func (am AdminServerManager) Logging(in *Nothing, stream Admin_LoggingServer) error {
	//for  {
	//	if err := stream.Send(Event{}); err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (am AdminServerManager) Statistics(in *StatInterval, stream Admin_StatisticsServer) error {

	//for  {
	//	if err := stream.Send(*Stat{}); err != nil {
	//		return err
	//	}
	//}
	return nil

}

func (as AdminServerManager) AuthLogin(consumer string, method string) error {
	if _, ok := as.ACLData[consumer]; ok {
		methods, ok := as.ACLData[consumer].([]interface{})
		if !ok {
			return fmt.Errorf("Argument is not a slice")
		}

		for _, val := range methods {
			v := strings.TrimRight(val.(string),"*")
			if strings.Contains(method,v) {
				return status.Error(codes.OK,"authenticated ok")
			}
		}

	}
	return status.Error(codes.Unauthenticated, "Unauthenticated")
}

type BizServerManager struct {
	ACLData map[string]interface{}
}

func NewBizServer(acldata string) (*BizServerManager, error) {
	adata := make(map[string]interface{})
	err := json.Unmarshal([]byte(acldata), &adata)
	if err != nil {
		return nil, err
	}

	return &BizServerManager{ACLData: adata}, nil
}

func (bz BizServerManager) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}
func (bz BizServerManager) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bz BizServerManager) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bz BizServerManager) AuthLogin(consumer string, method string) error {
	if _, ok := bz.ACLData[consumer]; ok {
		methods, ok := bz.ACLData[consumer].([]interface{})
		if !ok {
			return fmt.Errorf("Argument is not a slice")
		}

		for _, val := range methods {
			v := strings.TrimRight(val.(string),"*")
			if strings.Contains(method,v) {
				return status.Error(codes.OK,"authenticated ok")
			}
		}

	}
	return status.Error(codes.Unauthenticated, "Unauthenticated")
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {

	bizServer, err := NewBizServer(ACLData)
	if err != nil {
		return err
	}

	adminServer, err := NewAdminServerManager(ACLData)
	if err != nil {
		return err
	}

	go func() {
		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			log.Fatalln("cant listet port", err)
		}
		server := grpc.NewServer(grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(BizUnaryInterceptor)), grpc.InTapHandle(rateLimiter),grpc.StreamInterceptor(AdminStreamInterceptor))
		//server := grpc.NewServer(grpc.UnaryInterceptor(BizUnaryInterceptor), grpc.InTapHandle(rateLimiter),)
		RegisterBizServer(server, bizServer)
		RegisterAdminServer(server, adminServer)
		//RegisterAdminServer(server,NewAdminServer())
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

func BizUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler, ) (interface{}, error) {
	//	start := time.Now()

	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md["consumer"][0]

	ac := info.Server.(*BizServerManager)
	err := ac.AuthLogin(consumer, info.FullMethod)
	if grpc.Code(err) != codes.OK {
		return nil, err
	}
	reply, err := handler(ctx, req)

	//	fmt.Printf(`--
	//	after incoming call=%v
	//	req=%#v
	//	reply=%#v
	//	time=%v
	//	md=%v
	//	err=%v
	//`, info.FullMethod, req, reply, time.Since(start), md, err)
	return reply, err
}

func AdminStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (error) {
	//	start := time.Now()
	md, _ := metadata.FromIncomingContext(ss.Context())
	consumer := md["consumer"][0]
	//
	ac := srv.(*AdminServerManager)
	err := ac.AuthLogin(consumer, info.FullMethod)
	if grpc.Code(err) != codes.OK {
		return err
	}
	err = handler(srv, ss)

	//return reply, err
	return err
}

// -----

func rateLimiter(ctx context.Context, info *tap.Info) (context.Context, error) {
	fmt.Printf("--\ncheck ratelim for %s\n", info.FullMethodName)

	return ctx, nil
}
