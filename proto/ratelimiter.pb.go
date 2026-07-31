package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Priority int32

const (
	Priority_PRIORITY_UNSPECIFIED Priority = 0
	Priority_PRIORITY_CRITICAL    Priority = 1
	Priority_PRIORITY_STANDARD    Priority = 2
	Priority_PRIORITY_BEST_EFFORT Priority = 3
)

var (
	Priority_name = map[int32]string{
		0: "PRIORITY_UNSPECIFIED",
		1: "PRIORITY_CRITICAL",
		2: "PRIORITY_STANDARD",
		3: "PRIORITY_BEST_EFFORT",
	}
	Priority_value = map[string]int32{
		"PRIORITY_UNSPECIFIED": 0,
		"PRIORITY_CRITICAL":    1,
		"PRIORITY_STANDARD":    2,
		"PRIORITY_BEST_EFFORT": 3,
	}
)

func (x Priority) Enum() *Priority {
	p := new(Priority)
	*p = x
	return p
}

func (x Priority) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Priority) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_ratelimiter_proto_enumTypes[0].Descriptor()
}

func (Priority) Type() protoreflect.EnumType {
	return &file_proto_ratelimiter_proto_enumTypes[0]
}

func (x Priority) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (Priority) EnumDescriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{0}
}

type AllowRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ClientId      string                 `protobuf:"bytes,1,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`
	Cost          int64                  `protobuf:"varint,2,opt,name=cost,proto3" json:"cost,omitempty"`
	Priority      Priority               `protobuf:"varint,3,opt,name=priority,proto3,enum=ratelimiter.Priority" json:"priority,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AllowRequest) Reset() {
	*x = AllowRequest{}
	mi := &file_proto_ratelimiter_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AllowRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AllowRequest) ProtoMessage() {}

func (x *AllowRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*AllowRequest) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{0}
}

func (x *AllowRequest) GetClientId() string {
	if x != nil {
		return x.ClientId
	}
	return ""
}

func (x *AllowRequest) GetCost() int64 {
	if x != nil {
		return x.Cost
	}
	return 0
}

func (x *AllowRequest) GetPriority() Priority {
	if x != nil {
		return x.Priority
	}
	return Priority_PRIORITY_UNSPECIFIED
}

type AllowResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Allowed       bool                   `protobuf:"varint,1,opt,name=allowed,proto3" json:"allowed,omitempty"`
	Remaining     int64                  `protobuf:"varint,2,opt,name=remaining,proto3" json:"remaining,omitempty"`
	Reason        string                 `protobuf:"bytes,3,opt,name=reason,proto3" json:"reason,omitempty"`
	RetryAfter    *durationpb.Duration   `protobuf:"bytes,4,opt,name=retry_after,json=retryAfter,proto3" json:"retry_after,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AllowResponse) Reset() {
	*x = AllowResponse{}
	mi := &file_proto_ratelimiter_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AllowResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AllowResponse) ProtoMessage() {}

func (x *AllowResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*AllowResponse) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{1}
}

func (x *AllowResponse) GetAllowed() bool {
	if x != nil {
		return x.Allowed
	}
	return false
}

func (x *AllowResponse) GetRemaining() int64 {
	if x != nil {
		return x.Remaining
	}
	return 0
}

func (x *AllowResponse) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

func (x *AllowResponse) GetRetryAfter() *durationpb.Duration {
	if x != nil {
		return x.RetryAfter
	}
	return nil
}

type MemberInfo struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	NodeId        string                 `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Address       string                 `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	Status        int32                  `protobuf:"varint,3,opt,name=status,proto3" json:"status,omitempty"`
	Incarnation   int32                  `protobuf:"varint,4,opt,name=incarnation,proto3" json:"incarnation,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *MemberInfo) Reset() {
	*x = MemberInfo{}
	mi := &file_proto_ratelimiter_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MemberInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MemberInfo) ProtoMessage() {}

func (x *MemberInfo) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*MemberInfo) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{2}
}

func (x *MemberInfo) GetNodeId() string {
	if x != nil {
		return x.NodeId
	}
	return ""
}

func (x *MemberInfo) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

func (x *MemberInfo) GetStatus() int32 {
	if x != nil {
		return x.Status
	}
	return 0
}

func (x *MemberInfo) GetIncarnation() int32 {
	if x != nil {
		return x.Incarnation
	}
	return 0
}

type PingRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	NodeId        string                 `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	ListenAddress string                 `protobuf:"bytes,2,opt,name=listen_address,json=listenAddress,proto3" json:"listen_address,omitempty"`
	Members       []*MemberInfo          `protobuf:"bytes,3,rep,name=members,proto3" json:"members,omitempty"`
	TargetNodeId  string                 `protobuf:"bytes,4,opt,name=target_node_id,json=targetNodeId,proto3" json:"target_node_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PingRequest) Reset() {
	*x = PingRequest{}
	mi := &file_proto_ratelimiter_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PingRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingRequest) ProtoMessage() {}

func (x *PingRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PingRequest) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{3}
}

func (x *PingRequest) GetNodeId() string {
	if x != nil {
		return x.NodeId
	}
	return ""
}

func (x *PingRequest) GetListenAddress() string {
	if x != nil {
		return x.ListenAddress
	}
	return ""
}

func (x *PingRequest) GetMembers() []*MemberInfo {
	if x != nil {
		return x.Members
	}
	return nil
}

func (x *PingRequest) GetTargetNodeId() string {
	if x != nil {
		return x.TargetNodeId
	}
	return ""
}

type PingResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Ack           bool                   `protobuf:"varint,1,opt,name=ack,proto3" json:"ack,omitempty"`
	Members       []*MemberInfo          `protobuf:"bytes,2,rep,name=members,proto3" json:"members,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PingResponse) Reset() {
	*x = PingResponse{}
	mi := &file_proto_ratelimiter_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PingResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingResponse) ProtoMessage() {}

func (x *PingResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PingResponse) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{4}
}

func (x *PingResponse) GetAck() bool {
	if x != nil {
		return x.Ack
	}
	return false
}

func (x *PingResponse) GetMembers() []*MemberInfo {
	if x != nil {
		return x.Members
	}
	return nil
}

type GossipRequest struct {
	state         protoimpl.MessageState  `protogen:"open.v1"`
	SourceNodeId  string                  `protobuf:"bytes,1,opt,name=source_node_id,json=sourceNodeId,proto3" json:"source_node_id,omitempty"`
	State         map[string]*ClientState `protobuf:"bytes,2,rep,name=state,proto3" json:"state,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GossipRequest) Reset() {
	*x = GossipRequest{}
	mi := &file_proto_ratelimiter_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GossipRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GossipRequest) ProtoMessage() {}

func (x *GossipRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GossipRequest) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{5}
}

func (x *GossipRequest) GetSourceNodeId() string {
	if x != nil {
		return x.SourceNodeId
	}
	return ""
}

func (x *GossipRequest) GetState() map[string]*ClientState {
	if x != nil {
		return x.State
	}
	return nil
}

type ClientState struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Epochs        map[int64]*EpochState  `protobuf:"bytes,1,rep,name=epochs,proto3" json:"epochs,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClientState) Reset() {
	*x = ClientState{}
	mi := &file_proto_ratelimiter_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClientState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClientState) ProtoMessage() {}

func (x *ClientState) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ClientState) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{6}
}

func (x *ClientState) GetEpochs() map[int64]*EpochState {
	if x != nil {
		return x.Epochs
	}
	return nil
}

type EpochState struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	NodeCounts    map[string]int64       `protobuf:"bytes,1,rep,name=node_counts,json=nodeCounts,proto3" json:"node_counts,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *EpochState) Reset() {
	*x = EpochState{}
	mi := &file_proto_ratelimiter_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *EpochState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EpochState) ProtoMessage() {}

func (x *EpochState) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*EpochState) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{7}
}

func (x *EpochState) GetNodeCounts() map[string]int64 {
	if x != nil {
		return x.NodeCounts
	}
	return nil
}

type GossipResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Success       bool                   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GossipResponse) Reset() {
	*x = GossipResponse{}
	mi := &file_proto_ratelimiter_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GossipResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GossipResponse) ProtoMessage() {}

func (x *GossipResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_ratelimiter_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GossipResponse) Descriptor() ([]byte, []int) {
	return file_proto_ratelimiter_proto_rawDescGZIP(), []int{8}
}

func (x *GossipResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_proto_ratelimiter_proto protoreflect.FileDescriptor

const file_proto_ratelimiter_proto_rawDesc = "" +
	"\n" +
	"\x17proto/ratelimiter.proto\x12\vratelimiter\x1a\x1egoogle/protobuf/duration.proto\"r\n" +
	"\fAllowRequest\x12\x1b\n" +
	"\tclient_id\x18\x01 \x01(\tR\bclientId\x12\x12\n" +
	"\x04cost\x18\x02 \x01(\x03R\x04cost\x121\n" +
	"\bpriority\x18\x03 \x01(\x0e2\x15.ratelimiter.PriorityR\bpriority\"\x9b\x01\n" +
	"\rAllowResponse\x12\x18\n" +
	"\aallowed\x18\x01 \x01(\bR\aallowed\x12\x1c\n" +
	"\tremaining\x18\x02 \x01(\x03R\tremaining\x12\x16\n" +
	"\x06reason\x18\x03 \x01(\tR\x06reason\x12:\n" +
	"\vretry_after\x18\x04 \x01(\v2\x19.google.protobuf.DurationR\n" +
	"retryAfter\"y\n" +
	"\n" +
	"MemberInfo\x12\x17\n" +
	"\anode_id\x18\x01 \x01(\tR\x06nodeId\x12\x18\n" +
	"\aaddress\x18\x02 \x01(\tR\aaddress\x12\x16\n" +
	"\x06status\x18\x03 \x01(\x05R\x06status\x12 \n" +
	"\vincarnation\x18\x04 \x01(\x05R\vincarnation\"\xa6\x01\n" +
	"\vPingRequest\x12\x17\n" +
	"\anode_id\x18\x01 \x01(\tR\x06nodeId\x12%\n" +
	"\x0elisten_address\x18\x02 \x01(\tR\rlistenAddress\x121\n" +
	"\amembers\x18\x03 \x03(\v2\x17.ratelimiter.MemberInfoR\amembers\x12$\n" +
	"\x0etarget_node_id\x18\x04 \x01(\tR\ftargetNodeId\"S\n" +
	"\fPingResponse\x12\x10\n" +
	"\x03ack\x18\x01 \x01(\bR\x03ack\x121\n" +
	"\amembers\x18\x02 \x03(\v2\x17.ratelimiter.MemberInfoR\amembers\"\xc6\x01\n" +
	"\rGossipRequest\x12$\n" +
	"\x0esource_node_id\x18\x01 \x01(\tR\fsourceNodeId\x12;\n" +
	"\x05state\x18\x02 \x03(\v2%.ratelimiter.GossipRequest.StateEntryR\x05state\x1aR\n" +
	"\n" +
	"StateEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12.\n" +
	"\x05value\x18\x02 \x01(\v2\x18.ratelimiter.ClientStateR\x05value:\x028\x01\"\x9f\x01\n" +
	"\vClientState\x12<\n" +
	"\x06epochs\x18\x01 \x03(\v2$.ratelimiter.ClientState.EpochsEntryR\x06epochs\x1aR\n" +
	"\vEpochsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12-\n" +
	"\x05value\x18\x02 \x01(\v2\x17.ratelimiter.EpochStateR\x05value:\x028\x01\"\x95\x01\n" +
	"\n" +
	"EpochState\x12H\n" +
	"\vnode_counts\x18\x01 \x03(\v2'.ratelimiter.EpochState.NodeCountsEntryR\n" +
	"nodeCounts\x1a=\n" +
	"\x0fNodeCountsEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01\"*\n" +
	"\x0eGossipResponse\x12\x18\n" +
	"\asuccess\x18\x01 \x01(\bR\asuccess*l\n" +
	"\bPriority\x12\x18\n" +
	"\x14PRIORITY_UNSPECIFIED\x10\x00\x12\x15\n" +
	"\x11PRIORITY_CRITICAL\x10\x01\x12\x15\n" +
	"\x11PRIORITY_STANDARD\x10\x02\x12\x18\n" +
	"\x14PRIORITY_BEST_EFFORT\x10\x032\xd2\x01\n" +
	"\vRateLimiter\x12>\n" +
	"\x05Allow\x12\x19.ratelimiter.AllowRequest\x1a\x1a.ratelimiter.AllowResponse\x12;\n" +
	"\x04Ping\x12\x18.ratelimiter.PingRequest\x1a\x19.ratelimiter.PingResponse\x12F\n" +
	"\vGossipState\x12\x1a.ratelimiter.GossipRequest\x1a\x1b.ratelimiter.GossipResponseB4Z2github.com/sakthivel-p-cse/crdt-rate-limiter/protob\x06proto3"

var (
	file_proto_ratelimiter_proto_rawDescOnce sync.Once
	file_proto_ratelimiter_proto_rawDescData []byte
)

func file_proto_ratelimiter_proto_rawDescGZIP() []byte {
	file_proto_ratelimiter_proto_rawDescOnce.Do(func() {
		file_proto_ratelimiter_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_ratelimiter_proto_rawDesc), len(file_proto_ratelimiter_proto_rawDesc)))
	})
	return file_proto_ratelimiter_proto_rawDescData
}

var file_proto_ratelimiter_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_proto_ratelimiter_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_proto_ratelimiter_proto_goTypes = []any{
	(Priority)(0),
	(*AllowRequest)(nil),
	(*AllowResponse)(nil),
	(*MemberInfo)(nil),
	(*PingRequest)(nil),
	(*PingResponse)(nil),
	(*GossipRequest)(nil),
	(*ClientState)(nil),
	(*EpochState)(nil),
	(*GossipResponse)(nil),
	nil,
	nil,
	nil,
	(*durationpb.Duration)(nil),
}
var file_proto_ratelimiter_proto_depIdxs = []int32{
	0,
	13,
	3,
	3,
	10,
	11,
	12,
	7,
	8,
	1,
	4,
	6,
	2,
	5,
	9,
	12,
	9,
	9,
	9,
	0,
}

func init() { file_proto_ratelimiter_proto_init() }
func file_proto_ratelimiter_proto_init() {
	if File_proto_ratelimiter_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_ratelimiter_proto_rawDesc), len(file_proto_ratelimiter_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_ratelimiter_proto_goTypes,
		DependencyIndexes: file_proto_ratelimiter_proto_depIdxs,
		EnumInfos:         file_proto_ratelimiter_proto_enumTypes,
		MessageInfos:      file_proto_ratelimiter_proto_msgTypes,
	}.Build()
	File_proto_ratelimiter_proto = out.File
	file_proto_ratelimiter_proto_goTypes = nil
	file_proto_ratelimiter_proto_depIdxs = nil
}
