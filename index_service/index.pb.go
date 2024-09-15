// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: index.proto

package indexservice

import (
	context "context"
	fmt "fmt"
	types "github.com/atopos31/go-velisearch/types"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type DocId struct {
	DocId string `protobuf:"bytes,1,opt,name=DocId,proto3" json:"DocId,omitempty"`
}

func (m *DocId) Reset()         { *m = DocId{} }
func (m *DocId) String() string { return proto.CompactTextString(m) }
func (*DocId) ProtoMessage()    {}
func (*DocId) Descriptor() ([]byte, []int) {
	return fileDescriptor_f750e0f7889345b5, []int{0}
}
func (m *DocId) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *DocId) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_DocId.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *DocId) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DocId.Merge(m, src)
}
func (m *DocId) XXX_Size() int {
	return m.Size()
}
func (m *DocId) XXX_DiscardUnknown() {
	xxx_messageInfo_DocId.DiscardUnknown(m)
}

var xxx_messageInfo_DocId proto.InternalMessageInfo

func (m *DocId) GetDocId() string {
	if m != nil {
		return m.DocId
	}
	return ""
}

type AffectedCount struct {
	Count int32 `protobuf:"varint,1,opt,name=Count,proto3" json:"Count,omitempty"`
}

func (m *AffectedCount) Reset()         { *m = AffectedCount{} }
func (m *AffectedCount) String() string { return proto.CompactTextString(m) }
func (*AffectedCount) ProtoMessage()    {}
func (*AffectedCount) Descriptor() ([]byte, []int) {
	return fileDescriptor_f750e0f7889345b5, []int{1}
}
func (m *AffectedCount) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AffectedCount) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AffectedCount.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *AffectedCount) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AffectedCount.Merge(m, src)
}
func (m *AffectedCount) XXX_Size() int {
	return m.Size()
}
func (m *AffectedCount) XXX_DiscardUnknown() {
	xxx_messageInfo_AffectedCount.DiscardUnknown(m)
}

var xxx_messageInfo_AffectedCount proto.InternalMessageInfo

func (m *AffectedCount) GetCount() int32 {
	if m != nil {
		return m.Count
	}
	return 0
}

type SearchRequest struct {
	Query   *types.TermQuery `protobuf:"bytes,1,opt,name=Query,proto3" json:"Query,omitempty"`
	OnFlag  uint64           `protobuf:"varint,2,opt,name=OnFlag,proto3" json:"OnFlag,omitempty"`
	OffFlag uint64           `protobuf:"varint,3,opt,name=OffFlag,proto3" json:"OffFlag,omitempty"`
	OrFlags []uint64         `protobuf:"varint,4,rep,packed,name=OrFlags,proto3" json:"OrFlags,omitempty"`
}

func (m *SearchRequest) Reset()         { *m = SearchRequest{} }
func (m *SearchRequest) String() string { return proto.CompactTextString(m) }
func (*SearchRequest) ProtoMessage()    {}
func (*SearchRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f750e0f7889345b5, []int{2}
}
func (m *SearchRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SearchRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SearchRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SearchRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SearchRequest.Merge(m, src)
}
func (m *SearchRequest) XXX_Size() int {
	return m.Size()
}
func (m *SearchRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SearchRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SearchRequest proto.InternalMessageInfo

func (m *SearchRequest) GetQuery() *types.TermQuery {
	if m != nil {
		return m.Query
	}
	return nil
}

func (m *SearchRequest) GetOnFlag() uint64 {
	if m != nil {
		return m.OnFlag
	}
	return 0
}

func (m *SearchRequest) GetOffFlag() uint64 {
	if m != nil {
		return m.OffFlag
	}
	return 0
}

func (m *SearchRequest) GetOrFlags() []uint64 {
	if m != nil {
		return m.OrFlags
	}
	return nil
}

type SearchResult struct {
	Results []*types.Document `protobuf:"bytes,1,rep,name=Results,proto3" json:"Results,omitempty"`
}

func (m *SearchResult) Reset()         { *m = SearchResult{} }
func (m *SearchResult) String() string { return proto.CompactTextString(m) }
func (*SearchResult) ProtoMessage()    {}
func (*SearchResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_f750e0f7889345b5, []int{3}
}
func (m *SearchResult) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SearchResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SearchResult.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SearchResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SearchResult.Merge(m, src)
}
func (m *SearchResult) XXX_Size() int {
	return m.Size()
}
func (m *SearchResult) XXX_DiscardUnknown() {
	xxx_messageInfo_SearchResult.DiscardUnknown(m)
}

var xxx_messageInfo_SearchResult proto.InternalMessageInfo

func (m *SearchResult) GetResults() []*types.Document {
	if m != nil {
		return m.Results
	}
	return nil
}

type CountRequest struct {
}

func (m *CountRequest) Reset()         { *m = CountRequest{} }
func (m *CountRequest) String() string { return proto.CompactTextString(m) }
func (*CountRequest) ProtoMessage()    {}
func (*CountRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f750e0f7889345b5, []int{4}
}
func (m *CountRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *CountRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_CountRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *CountRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CountRequest.Merge(m, src)
}
func (m *CountRequest) XXX_Size() int {
	return m.Size()
}
func (m *CountRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_CountRequest.DiscardUnknown(m)
}

var xxx_messageInfo_CountRequest proto.InternalMessageInfo

func init() {
	proto.RegisterType((*DocId)(nil), "indexservice.DocId")
	proto.RegisterType((*AffectedCount)(nil), "indexservice.AffectedCount")
	proto.RegisterType((*SearchRequest)(nil), "indexservice.SearchRequest")
	proto.RegisterType((*SearchResult)(nil), "indexservice.SearchResult")
	proto.RegisterType((*CountRequest)(nil), "indexservice.CountRequest")
}

func init() { proto.RegisterFile("index.proto", fileDescriptor_f750e0f7889345b5) }

var fileDescriptor_f750e0f7889345b5 = []byte{
	// 359 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x52, 0x4d, 0x4b, 0xc3, 0x40,
	0x14, 0xec, 0xf6, 0x23, 0xa5, 0xaf, 0xa9, 0x96, 0x55, 0x24, 0x44, 0x0c, 0x21, 0xa0, 0xd4, 0x4b,
	0x0e, 0x15, 0x04, 0x0f, 0x82, 0xd5, 0x20, 0xf4, 0x54, 0x4c, 0xbd, 0x4b, 0x4d, 0x5e, 0xb4, 0xd0,
	0x66, 0xdb, 0xcd, 0x46, 0xec, 0xd9, 0x9b, 0x27, 0x7f, 0x96, 0xc7, 0x1e, 0x3d, 0x4a, 0xfb, 0x47,
	0x24, 0xbb, 0x29, 0xb4, 0x45, 0x7a, 0x9b, 0x79, 0x33, 0x8f, 0xcc, 0x9b, 0x2c, 0xd4, 0x87, 0x71,
	0x88, 0xef, 0xee, 0x84, 0x33, 0xc1, 0xa8, 0x2e, 0x49, 0x82, 0xfc, 0x6d, 0x18, 0xa0, 0x59, 0x0b,
	0x59, 0xa0, 0x04, 0xb3, 0x29, 0x90, 0x8f, 0x9f, 0xa6, 0x29, 0xf2, 0x99, 0x9a, 0x38, 0x27, 0x50,
	0xf1, 0x58, 0xd0, 0x0d, 0xe9, 0x61, 0x0e, 0x0c, 0x62, 0x93, 0x56, 0xcd, 0x57, 0xc4, 0x39, 0x85,
	0x46, 0x27, 0x8a, 0x30, 0x10, 0x18, 0xde, 0xb1, 0x34, 0x16, 0x99, 0x4d, 0x02, 0x69, 0xab, 0xf8,
	0x8a, 0x38, 0x1f, 0x04, 0x1a, 0x7d, 0x1c, 0xf0, 0xe0, 0xd5, 0xc7, 0x69, 0x8a, 0x89, 0xa0, 0x67,
	0x50, 0x79, 0xc8, 0x3e, 0x23, 0x7d, 0xf5, 0x76, 0xd3, 0x15, 0xb3, 0x09, 0x26, 0xee, 0x23, 0xf2,
	0xb1, 0x9c, 0xfb, 0x4a, 0xa6, 0x47, 0xa0, 0xf5, 0xe2, 0xfb, 0xd1, 0xe0, 0xc5, 0x28, 0xda, 0xa4,
	0x55, 0xf6, 0x73, 0x46, 0x0d, 0xa8, 0xf6, 0xa2, 0x48, 0x0a, 0x25, 0x29, 0xac, 0xa8, 0x54, 0x78,
	0x86, 0x12, 0xa3, 0x6c, 0x97, 0xa4, 0xa2, 0xa8, 0x73, 0x05, 0xfa, 0x2a, 0x44, 0x92, 0x8e, 0x04,
	0x3d, 0x87, 0xaa, 0x42, 0x89, 0x41, 0xec, 0x52, 0xab, 0xde, 0xde, 0xcf, 0x53, 0x78, 0x2c, 0x48,
	0xc7, 0x18, 0x0b, 0x7f, 0xa5, 0x3b, 0x7b, 0xa0, 0xcb, 0x4b, 0xf2, 0xf8, 0xed, 0xcf, 0x22, 0xe8,
	0xdd, 0xac, 0xc4, 0xbe, 0x2a, 0x91, 0x5e, 0x43, 0xcd, 0xc3, 0x11, 0x0a, 0xf4, 0x58, 0x40, 0x0f,
	0xdc, 0xf5, 0x82, 0x5d, 0x59, 0x95, 0x79, 0xbc, 0x39, 0xdc, 0xac, 0xed, 0x12, 0xb4, 0x4e, 0x18,
	0x66, 0xbb, 0xdb, 0x19, 0x76, 0xef, 0x75, 0x40, 0x53, 0x27, 0xd1, 0x2d, 0xdb, 0x46, 0xdb, 0xa6,
	0xf9, 0xbf, 0x28, 0x5b, 0xb8, 0xc9, 0xff, 0x18, 0xdd, 0x32, 0xad, 0xdf, 0xbb, 0x33, 0xc4, 0xad,
	0xf1, 0xbd, 0xb0, 0xc8, 0x7c, 0x61, 0x91, 0xdf, 0x85, 0x45, 0xbe, 0x96, 0x56, 0x61, 0xbe, 0xb4,
	0x0a, 0x3f, 0x4b, 0xab, 0xf0, 0xac, 0xc9, 0x47, 0x74, 0xf1, 0x17, 0x00, 0x00, 0xff, 0xff, 0x77,
	0x5c, 0x4a, 0x05, 0x7e, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// IndexServiceClient is the client API for IndexService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type IndexServiceClient interface {
	DeleteDoc(ctx context.Context, in *DocId, opts ...grpc.CallOption) (*AffectedCount, error)
	AddDoc(ctx context.Context, in *types.Document, opts ...grpc.CallOption) (*AffectedCount, error)
	Search(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (*SearchResult, error)
	Count(ctx context.Context, in *CountRequest, opts ...grpc.CallOption) (*AffectedCount, error)
}

type indexServiceClient struct {
	cc *grpc.ClientConn
}

func NewIndexServiceClient(cc *grpc.ClientConn) IndexServiceClient {
	return &indexServiceClient{cc}
}

func (c *indexServiceClient) DeleteDoc(ctx context.Context, in *DocId, opts ...grpc.CallOption) (*AffectedCount, error) {
	out := new(AffectedCount)
	err := c.cc.Invoke(ctx, "/indexservice.IndexService/DeleteDoc", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *indexServiceClient) AddDoc(ctx context.Context, in *types.Document, opts ...grpc.CallOption) (*AffectedCount, error) {
	out := new(AffectedCount)
	err := c.cc.Invoke(ctx, "/indexservice.IndexService/AddDoc", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *indexServiceClient) Search(ctx context.Context, in *SearchRequest, opts ...grpc.CallOption) (*SearchResult, error) {
	out := new(SearchResult)
	err := c.cc.Invoke(ctx, "/indexservice.IndexService/Search", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *indexServiceClient) Count(ctx context.Context, in *CountRequest, opts ...grpc.CallOption) (*AffectedCount, error) {
	out := new(AffectedCount)
	err := c.cc.Invoke(ctx, "/indexservice.IndexService/Count", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// IndexServiceServer is the server API for IndexService service.
type IndexServiceServer interface {
	DeleteDoc(context.Context, *DocId) (*AffectedCount, error)
	AddDoc(context.Context, *types.Document) (*AffectedCount, error)
	Search(context.Context, *SearchRequest) (*SearchResult, error)
	Count(context.Context, *CountRequest) (*AffectedCount, error)
}

// UnimplementedIndexServiceServer can be embedded to have forward compatible implementations.
type UnimplementedIndexServiceServer struct {
}

func (*UnimplementedIndexServiceServer) DeleteDoc(ctx context.Context, req *DocId) (*AffectedCount, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteDoc not implemented")
}
func (*UnimplementedIndexServiceServer) AddDoc(ctx context.Context, req *types.Document) (*AffectedCount, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddDoc not implemented")
}
func (*UnimplementedIndexServiceServer) Search(ctx context.Context, req *SearchRequest) (*SearchResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Search not implemented")
}
func (*UnimplementedIndexServiceServer) Count(ctx context.Context, req *CountRequest) (*AffectedCount, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Count not implemented")
}

func RegisterIndexServiceServer(s *grpc.Server, srv IndexServiceServer) {
	s.RegisterService(&_IndexService_serviceDesc, srv)
}

func _IndexService_DeleteDoc_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DocId)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndexServiceServer).DeleteDoc(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/indexservice.IndexService/DeleteDoc",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndexServiceServer).DeleteDoc(ctx, req.(*DocId))
	}
	return interceptor(ctx, in, info, handler)
}

func _IndexService_AddDoc_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.Document)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndexServiceServer).AddDoc(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/indexservice.IndexService/AddDoc",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndexServiceServer).AddDoc(ctx, req.(*types.Document))
	}
	return interceptor(ctx, in, info, handler)
}

func _IndexService_Search_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SearchRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndexServiceServer).Search(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/indexservice.IndexService/Search",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndexServiceServer).Search(ctx, req.(*SearchRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _IndexService_Count_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(IndexServiceServer).Count(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/indexservice.IndexService/Count",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(IndexServiceServer).Count(ctx, req.(*CountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _IndexService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "indexservice.IndexService",
	HandlerType: (*IndexServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DeleteDoc",
			Handler:    _IndexService_DeleteDoc_Handler,
		},
		{
			MethodName: "AddDoc",
			Handler:    _IndexService_AddDoc_Handler,
		},
		{
			MethodName: "Search",
			Handler:    _IndexService_Search_Handler,
		},
		{
			MethodName: "Count",
			Handler:    _IndexService_Count_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "index.proto",
}

func (m *DocId) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *DocId) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *DocId) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.DocId) > 0 {
		i -= len(m.DocId)
		copy(dAtA[i:], m.DocId)
		i = encodeVarintIndex(dAtA, i, uint64(len(m.DocId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *AffectedCount) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AffectedCount) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *AffectedCount) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Count != 0 {
		i = encodeVarintIndex(dAtA, i, uint64(m.Count))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *SearchRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SearchRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SearchRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.OrFlags) > 0 {
		dAtA2 := make([]byte, len(m.OrFlags)*10)
		var j1 int
		for _, num := range m.OrFlags {
			for num >= 1<<7 {
				dAtA2[j1] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j1++
			}
			dAtA2[j1] = uint8(num)
			j1++
		}
		i -= j1
		copy(dAtA[i:], dAtA2[:j1])
		i = encodeVarintIndex(dAtA, i, uint64(j1))
		i--
		dAtA[i] = 0x22
	}
	if m.OffFlag != 0 {
		i = encodeVarintIndex(dAtA, i, uint64(m.OffFlag))
		i--
		dAtA[i] = 0x18
	}
	if m.OnFlag != 0 {
		i = encodeVarintIndex(dAtA, i, uint64(m.OnFlag))
		i--
		dAtA[i] = 0x10
	}
	if m.Query != nil {
		{
			size, err := m.Query.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintIndex(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *SearchResult) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SearchResult) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SearchResult) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Results) > 0 {
		for iNdEx := len(m.Results) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Results[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintIndex(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *CountRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *CountRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *CountRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func encodeVarintIndex(dAtA []byte, offset int, v uint64) int {
	offset -= sovIndex(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *DocId) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.DocId)
	if l > 0 {
		n += 1 + l + sovIndex(uint64(l))
	}
	return n
}

func (m *AffectedCount) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Count != 0 {
		n += 1 + sovIndex(uint64(m.Count))
	}
	return n
}

func (m *SearchRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Query != nil {
		l = m.Query.Size()
		n += 1 + l + sovIndex(uint64(l))
	}
	if m.OnFlag != 0 {
		n += 1 + sovIndex(uint64(m.OnFlag))
	}
	if m.OffFlag != 0 {
		n += 1 + sovIndex(uint64(m.OffFlag))
	}
	if len(m.OrFlags) > 0 {
		l = 0
		for _, e := range m.OrFlags {
			l += sovIndex(uint64(e))
		}
		n += 1 + sovIndex(uint64(l)) + l
	}
	return n
}

func (m *SearchResult) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Results) > 0 {
		for _, e := range m.Results {
			l = e.Size()
			n += 1 + l + sovIndex(uint64(l))
		}
	}
	return n
}

func (m *CountRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func sovIndex(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozIndex(x uint64) (n int) {
	return sovIndex(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *DocId) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: DocId: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: DocId: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field DocId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthIndex
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthIndex
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.DocId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipIndex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthIndex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *AffectedCount) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: AffectedCount: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AffectedCount: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Count", wireType)
			}
			m.Count = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Count |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipIndex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthIndex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *SearchRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SearchRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SearchRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Query", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIndex
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIndex
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Query == nil {
				m.Query = &types.TermQuery{}
			}
			if err := m.Query.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field OnFlag", wireType)
			}
			m.OnFlag = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.OnFlag |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field OffFlag", wireType)
			}
			m.OffFlag = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.OffFlag |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowIndex
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.OrFlags = append(m.OrFlags, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowIndex
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthIndex
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthIndex
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.OrFlags) == 0 {
					m.OrFlags = make([]uint64, 0, elementCount)
				}
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowIndex
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.OrFlags = append(m.OrFlags, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field OrFlags", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipIndex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthIndex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *SearchResult) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SearchResult: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SearchResult: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Results", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIndex
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIndex
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Results = append(m.Results, &types.Document{})
			if err := m.Results[len(m.Results)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipIndex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthIndex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *CountRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: CountRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: CountRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipIndex(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthIndex
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipIndex(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowIndex
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowIndex
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthIndex
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupIndex
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthIndex
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthIndex        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowIndex          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupIndex = fmt.Errorf("proto: unexpected end of group")
)