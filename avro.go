package dkafka

// Schema is represented in JSON by one of:
// - A JSON string, naming a defined type.
// - A JSON object, of the form:
// - {"type": "typeName" ...attributes...}
// where typeName is either a primitive or derived type name, as defined below. Attributes not defined in this document are permitted as metadata, but must not affect the format of serialized data.
// A JSON array, representing a union of embedded types.
type Schema = interface{}

type Meta struct {
	Compatibility string `json:"name"`
	Type          string `json:"type"`
	Version       string `json:"version"`
}

type Message struct {
	Record
	Meta Meta `json:"meta"`
}

type Field struct {
	// Name a JSON string providing the name of the field (required)
	Name string `json:"name"`
	// Doc a JSON string describing this field for users (optional).
	Doc string `json:"doc,omitempty"`
	// Type a schema, as defined above
	Type Schema `json:"type"`
}

type Record struct {
	// Name a JSON string providing the name of the record (required)
	Name string `json:"name"`
	// Namespace a JSON string that qualifies the name
	Namespace string `json:"namespace,omitempty"`
	// Doc a JSON string providing documentation to the user of this schema (optional).
	Doc string `json:"doc,omitempty"`
	// Fields a JSON array, listing fields (required). Each field is a JSON object.
	Fields []Field `json:"fields,omitempty"`
}

type Array struct {
	// type always equal to "array"
	Type string `json:"type"`
	// items the schema of the array's items.
	Items Schema
	// todo manage default
}

func NewArray(itemType Schema) Array {
	return Array{
		Type:  "array",
		Items: itemType,
	}
}

type Union = []Schema

func NewOptional(schema Schema) Union {
	return []Schema{"null", schema}
}
