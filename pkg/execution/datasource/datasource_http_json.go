package datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
	log "github.com/jensneuse/abstractlogger"
	"github.com/jensneuse/graphql-go-tools/pkg/ast"
	"github.com/jensneuse/graphql-go-tools/pkg/lexer/literal"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HttpJsonDataSourceConfig is the configuration object for the HttpJsonDataSource
type HttpJsonDataSourceConfig struct {
	// Host is the hostname of the upstream
	Host string
	// URL is the url of the upstream
	URL string
	// Method is the http.Method, e.g. GET, POST, UPDATE, DELETE
	// default is GET
	Method *string
	// Body is the http body to send
	// default is null/nil (no body)
	Body *string
	// Headers defines the header mappings
	Headers []HttpJsonDataSourceConfigHeader
	// DefaultTypeName is the optional variable to define a default type name for the response object
	// This is useful in case the response might be a Union or Interface type which uses StatusCodeTypeNameMappings
	DefaultTypeName *string
	// StatusCodeTypeNameMappings is a slice of mappings from http.StatusCode to GraphQL TypeName
	// This can be used when the TypeName depends on the http.StatusCode
	StatusCodeTypeNameMappings []StatusCodeTypeNameMapping
}

type StatusCodeTypeNameMapping struct {
	StatusCode int
	TypeName   string
}

type HttpJsonDataSourceConfigHeader struct {
	Key   string
	Value string
}

type HttpJsonDataSourcePlannerFactoryFactory struct {
}

func (h HttpJsonDataSourcePlannerFactoryFactory) Initialize(base BasePlanner, configReader io.Reader) (PlannerFactory, error) {
	factory := HttpJsonDataSourcePlannerFactory{
		base: base,
	}
	err := json.NewDecoder(configReader).Decode(&factory.config)
	return factory, err
}

type HttpJsonDataSourcePlannerFactory struct {
	base   BasePlanner
	config HttpJsonDataSourceConfig
}

func (h HttpJsonDataSourcePlannerFactory) DataSourcePlanner() Planner {
	return &HttpJsonDataSourcePlanner{
		BasePlanner:      h.base,
		dataSourceConfig: h.config,
	}
}

type HttpJsonDataSourcePlanner struct {
	BasePlanner
	dataSourceConfig HttpJsonDataSourceConfig
}

func (h *HttpJsonDataSourcePlanner) Plan(args []Argument) (DataSource, []Argument) {
	return &HttpJsonDataSource{
		Log: h.Log,
	}, append(h.Args, args...)
}

func (h *HttpJsonDataSourcePlanner) EnterInlineFragment(ref int) {

}

func (h *HttpJsonDataSourcePlanner) LeaveInlineFragment(ref int) {

}

func (h *HttpJsonDataSourcePlanner) EnterSelectionSet(ref int) {

}

func (h *HttpJsonDataSourcePlanner) LeaveSelectionSet(ref int) {

}

func (h *HttpJsonDataSourcePlanner) EnterField(ref int) {
	h.RootField.SetIfNotDefined(ref)
}

func (h *HttpJsonDataSourcePlanner) LeaveField(ref int) {
	if !h.RootField.IsDefinedAndEquals(ref) {
		return
	}
	definition, exists := h.Walker.FieldDefinition(ref)
	if !exists {
		return
	}
	h.Args = append(h.Args, &StaticVariableArgument{
		Name:  literal.HOST,
		Value: []byte(h.dataSourceConfig.Host),
	})
	h.Args = append(h.Args, &StaticVariableArgument{
		Name:  literal.URL,
		Value: []byte(h.dataSourceConfig.URL),
	})
	if h.dataSourceConfig.Method == nil {
		h.Args = append(h.Args, &StaticVariableArgument{
			Name:  literal.METHOD,
			Value: literal.HTTP_METHOD_GET,
		})
	} else {
		h.Args = append(h.Args, &StaticVariableArgument{
			Name:  literal.METHOD,
			Value: []byte(*h.dataSourceConfig.Method),
		})
	}
	if h.dataSourceConfig.Body != nil {
		h.Args = append(h.Args, &StaticVariableArgument{
			Name:  literal.BODY,
			Value: []byte(*h.dataSourceConfig.Body),
		})
	}

	if len(h.dataSourceConfig.Headers) != 0 {
		listArg := &ListArgument{
			Name: literal.HEADERS,
		}
		for i := range h.dataSourceConfig.Headers {
			listArg.Arguments = append(listArg.Arguments, &StaticVariableArgument{
				Name:  []byte(h.dataSourceConfig.Headers[i].Key),
				Value: []byte(h.dataSourceConfig.Headers[i].Value),
			})
		}
		h.Args = append(h.Args, listArg)
	}

	// __typename
	var typeNameValue []byte
	var err error
	fieldDefinitionTypeNode := h.Definition.FieldDefinitionTypeNode(definition)
	fieldDefinitionType := h.Definition.FieldDefinitionType(definition)
	fieldDefinitionTypeName := h.Definition.ResolveTypeName(fieldDefinitionType)
	quotedFieldDefinitionTypeName := append(literal.QUOTE, append(fieldDefinitionTypeName, literal.QUOTE...)...)
	switch fieldDefinitionTypeNode.Kind {
	case ast.NodeKindScalarTypeDefinition:
		return
	case ast.NodeKindUnionTypeDefinition, ast.NodeKindInterfaceTypeDefinition:
		if h.dataSourceConfig.DefaultTypeName != nil {
			typeNameValue, err = sjson.SetRawBytes(typeNameValue, "defaultTypeName", []byte("\""+*h.dataSourceConfig.DefaultTypeName+"\""))
			if err != nil {
				h.Log.Error("HttpJsonDataSourcePlanner set defaultTypeName (switch case union/interface)", log.Error(err))
				return
			}
		}
		for i := range h.dataSourceConfig.StatusCodeTypeNameMappings {
			typeNameValue, err = sjson.SetRawBytes(typeNameValue, strconv.Itoa(h.dataSourceConfig.StatusCodeTypeNameMappings[i].StatusCode), []byte("\""+h.dataSourceConfig.StatusCodeTypeNameMappings[i].TypeName+"\""))
			if err != nil {
				h.Log.Error("HttpJsonDataSourcePlanner set statusCodeTypeMapping", log.Error(err))
				return
			}
		}
	default:
		typeNameValue, err = sjson.SetRawBytes(typeNameValue, "defaultTypeName", quotedFieldDefinitionTypeName)
		if err != nil {
			h.Log.Error("HttpJsonDataSourcePlanner set defaultTypeName (switch case default)", log.Error(err))
			return
		}
	}
	h.Args = append(h.Args, &StaticVariableArgument{
		Name:  literal.TYPENAME,
		Value: typeNameValue,
	})
}

type HttpJsonDataSource struct {
	Log log.Logger
}

func (r *HttpJsonDataSource) Resolve(ctx context.Context, args ResolverArgs, out io.Writer) (n int, err error) {

	hostArg := args.ByKey(literal.HOST)
	urlArg := args.ByKey(literal.URL)
	methodArg := args.ByKey(literal.METHOD)
	bodyArg := args.ByKey(literal.BODY)
	headersArg := args.ByKey(literal.HEADERS)
	typeNameArg := args.ByKey(literal.TYPENAME)

	r.Log.Debug("HttpJsonDataSource.Resolve.Args",
		log.Strings("resolvedArgs", args.Dump()),
	)

	switch {
	case hostArg == nil:
		r.Log.Error(fmt.Sprintf("arg '%s' must not be nil", string(literal.HOST)))
		return
	case urlArg == nil:
		r.Log.Error(fmt.Sprintf("arg '%s' must not be nil", string(literal.URL)))
		return
	case methodArg == nil:
		r.Log.Error(fmt.Sprintf("arg '%s' must not be nil", string(literal.METHOD)))
		return
	}

	httpMethod := http.MethodGet
	switch {
	case bytes.Equal(methodArg, literal.HTTP_METHOD_GET):
		httpMethod = http.MethodGet
	case bytes.Equal(methodArg, literal.HTTP_METHOD_POST):
		httpMethod = http.MethodPost
	case bytes.Equal(methodArg, literal.HTTP_METHOD_PUT):
		httpMethod = http.MethodPut
	case bytes.Equal(methodArg, literal.HTTP_METHOD_DELETE):
		httpMethod = http.MethodDelete
	case bytes.Equal(methodArg, literal.HTTP_METHOD_PATCH):
		httpMethod = http.MethodPatch
	}

	url := string(hostArg) + string(urlArg)
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		url = "https://" + url
	}

	header := make(http.Header)
	if len(headersArg) != 0 {
		err := jsonparser.ObjectEach(headersArg, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			header.Set(string(key), string(value))
			return nil
		})
		if err != nil {
			r.Log.Error("accessing headers", log.Error(err))
		}
	}

	r.Log.Debug("HttpJsonDataSource.Resolve",
		log.String("url", url),
	)

	client := http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1024,
			TLSHandshakeTimeout: 0 * time.Second,
		},
	}

	var bodyReader io.Reader
	if len(bodyArg) != 0 {
		bodyArg = bytes.ReplaceAll(bodyArg, literal.BACKSLASH, nil)
		bodyReader = bytes.NewReader(bodyArg)
	}

	request, err := http.NewRequest(httpMethod, url, bodyReader)
	if err != nil {
		r.Log.Error("HttpJsonDataSource.Resolve.NewRequest",
			log.Error(err),
		)
		return
	}

	request.Header = header

	res, err := client.Do(request)
	if err != nil {
		r.Log.Error("HttpJsonDataSource.Resolve.client.Do",
			log.Error(err),
		)
		return
	}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		r.Log.Error("HttpJsonDataSource.Resolve.ioutil.ReadAll",
			log.Error(err),
		)
		return
	}

	statusCode := strconv.Itoa(res.StatusCode)
	statusCodeTypeName := gjson.GetBytes(typeNameArg, statusCode)
	if statusCodeTypeName.Exists() {
		data, err = sjson.SetRawBytes(data, "__typename", []byte(statusCodeTypeName.Raw))
		if err != nil {
			r.Log.Error("HttpJsonDataSource.Resolve.setStatusCodeTypeName",
				log.Error(err),
			)
			return
		}
	} else {
		defaultTypeName := gjson.GetBytes(typeNameArg, "defaultTypeName")
		if defaultTypeName.Exists() {
			data, err = sjson.SetRawBytes(data, "__typename", []byte(defaultTypeName.Raw))
			if err != nil {
				r.Log.Error("HttpJsonDataSource.Resolve.setDefaultTypeName",
					log.Error(err),
				)
				return
			}
		}
	}

	return out.Write(data)
}
