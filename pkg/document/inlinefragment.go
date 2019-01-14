package document

// InlineFragment as specified in:
// http://facebook.github.io/graphql/draft/#InlineFragment
type InlineFragment struct {
	TypeCondition int
	Directives    []int
	SelectionSet  SelectionSet
}

func (i InlineFragment) NodeValueType() ValueType {
	panic("implement me")
}

func (i InlineFragment) NodeValueReference() int {
	panic("implement me")
}

func (i InlineFragment) NodeUnionMemberTypes() []ByteSlice {
	panic("implement me")
}

func (i InlineFragment) NodeSchemaDefinition() SchemaDefinition {
	panic("implement me")
}

func (i InlineFragment) NodeScalarTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeObjectTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeInterfaceTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeUnionTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeEnumTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeInputObjectTypeDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeDirectiveDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeImplementsInterfaces() []ByteSlice {
	panic("implement me")
}

func (i InlineFragment) NodeValue() int {
	panic("implement me")
}

func (i InlineFragment) NodeDefaultValue() int {
	panic("implement me")
}

func (i InlineFragment) NodeAlias() string {
	panic("implement me")
}

func (i InlineFragment) NodeArgumentsDefinition() []int {
	panic("implement me")
}

func (i InlineFragment) NodeFieldsDefinition() []int {
	panic("implement me")
}

func (i InlineFragment) NodeOperationType() OperationType {
	panic("implement me")
}

func (i InlineFragment) NodeName() string {
	panic("implement me")
}

func (i InlineFragment) NodeDescription() string {
	panic("implement me")
}

func (i InlineFragment) NodeArguments() []int {
	panic("implement me")
}

func (i InlineFragment) NodeDirectives() []int {
	return i.Directives
}

func (i InlineFragment) NodeEnumValuesDefinition() []int {
	panic("implement me")
}

func (i InlineFragment) NodeFields() []int {
	return i.SelectionSet.Fields
}

func (i InlineFragment) NodeFragmentSpreads() []int {
	return i.SelectionSet.FragmentSpreads
}

func (i InlineFragment) NodeInlineFragments() []int {
	return i.SelectionSet.InlineFragments
}

func (i InlineFragment) NodeVariableDefinitions() []int {
	panic("implement me")
}

func (i InlineFragment) NodeType() int {
	return i.TypeCondition
}

// InlineFragments is the plural of InlineFragment
type InlineFragments []InlineFragment
