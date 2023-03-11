package decoder

import (
	"fmt"
	"strings"

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

	parser := &protoparse.Parser{
		ImportPaths:           []string{},
		IncludeSourceCodeInfo: true,
	}

	customFiles, err := parser.ParseFiles(protoPath)
	if err != nil {
		return nil, fmt.Errorf("parse proto file %q: %w", protoPath, err)

	}
	if len(customFiles) != 1 {
		return nil, fmt.Errorf("expected 1 proto file descriptor")
	}

	fileDescs, err := desc.CreateFileDescriptor(customFiles[0].AsFileDescriptorProto())
	if err != nil {
		return nil, fmt.Errorf("couldn't convert file descriptor proto to file descriptor: %w", err)
	}

	messageDescriptor := fileDescs.FindMessage(messageType)
	if messageDescriptor == nil {
		return nil, fmt.Errorf("failed to find message descriptor %q", messageType)
	}

	return &ProtoDecoder{
		messageType:       messageType,
		messageDescriptor: messageDescriptor,
	}, nil

}
