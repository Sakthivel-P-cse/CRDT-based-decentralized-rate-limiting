package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	RateLimiter_Allow_FullMethodName       = "/ratelimiter.RateLimiter/Allow"
	RateLimiter_Ping_FullMethodName        = "/ratelimiter.RateLimiter/Ping"
	RateLimiter_GossipState_FullMethodName = "/ratelimiter.RateLimiter/GossipState"
)

type RateLimiterClient interface {
	Allow(ctx context.Context, in *AllowRequest, opts ...grpc.CallOption) (*AllowResponse, error)
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
	GossipState(ctx context.Context, in *GossipRequest, opts ...grpc.CallOption) (*GossipResponse, error)
}

type rateLimiterClient struct {
	cc grpc.ClientConnInterface
}

func NewRateLimiterClient(cc grpc.ClientConnInterface) RateLimiterClient {
	return &rateLimiterClient{cc}
}

func (c *rateLimiterClient) Allow(ctx context.Context, in *AllowRequest, opts ...grpc.CallOption) (*AllowResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AllowResponse)
	err := c.cc.Invoke(ctx, RateLimiter_Allow_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *rateLimiterClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, RateLimiter_Ping_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *rateLimiterClient) GossipState(ctx context.Context, in *GossipRequest, opts ...grpc.CallOption) (*GossipResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GossipResponse)
	err := c.cc.Invoke(ctx, RateLimiter_GossipState_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type RateLimiterServer interface {
	Allow(context.Context, *AllowRequest) (*AllowResponse, error)
	Ping(context.Context, *PingRequest) (*PingResponse, error)
	GossipState(context.Context, *GossipRequest) (*GossipResponse, error)
	mustEmbedUnimplementedRateLimiterServer()
}

type UnimplementedRateLimiterServer struct{}

func (UnimplementedRateLimiterServer) Allow(context.Context, *AllowRequest) (*AllowResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Allow not implemented")
}
func (UnimplementedRateLimiterServer) Ping(context.Context, *PingRequest) (*PingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedRateLimiterServer) GossipState(context.Context, *GossipRequest) (*GossipResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GossipState not implemented")
}
func (UnimplementedRateLimiterServer) mustEmbedUnimplementedRateLimiterServer() {}
func (UnimplementedRateLimiterServer) testEmbeddedByValue()                     {}

type UnsafeRateLimiterServer interface {
	mustEmbedUnimplementedRateLimiterServer()
}

func RegisterRateLimiterServer(s grpc.ServiceRegistrar, srv RateLimiterServer) {

	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RateLimiter_ServiceDesc, srv)
}

func _RateLimiter_Allow_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AllowRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RateLimiterServer).Allow(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RateLimiter_Allow_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RateLimiterServer).Allow(ctx, req.(*AllowRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RateLimiter_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RateLimiterServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RateLimiter_Ping_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RateLimiterServer).Ping(ctx, req.(*PingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RateLimiter_GossipState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GossipRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RateLimiterServer).GossipState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RateLimiter_GossipState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RateLimiterServer).GossipState(ctx, req.(*GossipRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var RateLimiter_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ratelimiter.RateLimiter",
	HandlerType: (*RateLimiterServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Allow",
			Handler:    _RateLimiter_Allow_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _RateLimiter_Ping_Handler,
		},
		{
			MethodName: "GossipState",
			Handler:    _RateLimiter_GossipState_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/ratelimiter.proto",
}
