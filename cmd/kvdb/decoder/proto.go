package decoder

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/kvdb/cmd/kvdb/decoder/pb/system"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

var _ Decode = (*ProtoDecoder)(nil)

// proto:///path/to/file.proto@<full_qualified_message_type>
type ProtoDecoder struct {
	messageDescriptor *desc.MessageDescriptor
	messageType       string
}

func (p *ProtoDecoder) Decode(bytes []byte) string {
	dynMsg := dynamic.NewMessageFactoryWithDefaults().NewDynamicMessage(p.messageDescriptor)
	if err := dynMsg.Unmarshal(bytes); err != nil {
		return fmt.Sprintf("Error unmarshalling message into %s: %s\n", p.messageType, err.Error())

	}

	cnt, err := dynMsg.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("Error marhsalling proto to json %s: %s\n", p.messageType, err.Error())
	}
	return string(cnt)
}

func newProtoDecoder(scheme string) (*ProtoDecoder, error) {
	chunks := strings.Split(scheme, "://")
	if len(chunks) != 2 {
		return nil, fmt.Errorf("invalid proto decoder scheme %q, expect proto:///path/to/file.proto@<full_qualified_message_type>", scheme)
	}

	protoChunks := strings.Split(chunks[1], "@")
	if len(chunks) != 2 {
		return nil, fmt.Errorf("invalid proto decoder scheme %q, expect proto:///path/to/file.proto@<full_qualified_message_type>", scheme)
	}

	protoPath := protoChunks[0]
	messageType := protoChunks[1]

	protoFiles, err := loadProtobufs(protoPath)
	if err != nil {
		return nil, fmt.Errorf("load protos: %w", err)

	}

	fileDescs, err := desc.CreateFileDescriptors(protoFiles)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert, should do this check much earlier: %w", err)
	}

	var msgDesc *desc.MessageDescriptor
	for _, file := range fileDescs {
		msgDesc = file.FindMessage(messageType)
		if msgDesc != nil {
			break
		}
	}

	if msgDesc == nil {
		return nil, fmt.Errorf("failed to find message descriptor %q", messageType)
	}

	return &ProtoDecoder{
		messageType:       messageType,
		messageDescriptor: msgDesc,
	}, nil

}

func loadProtobufs(protoPath string) (out []*descriptorpb.FileDescriptorProto, err error) {
	// System protos
	systemFiles, err := readSystemProtobufs()
	if err != nil {
		return nil, err
	}

	for _, file := range systemFiles.File {
		out = append(out, file)
	}

	// User-specified protos
	parser := &protoparse.Parser{
		ImportPaths:           []string{},
		IncludeSourceCodeInfo: true,
	}

	customFiles, err := parser.ParseFiles(protoPath)
	if err != nil {
		return nil, fmt.Errorf("parse proto file %q: %w", protoPath, err)
	}

	for _, fd := range customFiles {
		out = append(out, fd.AsFileDescriptorProto())
	}

	return out, nil
}

func readSystemProtobufs() (*descriptorpb.FileDescriptorSet, error) {
	fds := &descriptorpb.FileDescriptorSet{}
	err := proto.Unmarshal(system.ProtobufDescriptors, fds)
	if err != nil {
		return nil, err
	}

	return fds, nil
}
