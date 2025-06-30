// Type utilities
export type NonNullable<T> = T extends null | undefined ? never : T;
export type RequiredKeys<T extends SchemaDefinition> = {
  // A key is required if its field type includes `required: true` specifically.
  [K in keyof T]: T[K] extends { required: true } ? K : never;
}[keyof T];

// Every other key is optional
export type OptionalKeys<T extends SchemaDefinition> = Exclude<
  keyof T,
  RequiredKeys<T>
>;

// Schema definition types
export type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "date"
  | "array"
  | "object"
  | "set";

export interface BaseField {
  type: FieldType;
  required?: boolean;
  default?: any;
  validate?: (value: any) => boolean | string;
}

export interface StringField extends BaseField {
  type: "string";
  minLength?: number;
  maxLength?: number;
  pattern?: RegExp;
}

export interface NumberField extends BaseField {
  type: "number";
  min?: number;
  max?: number;
  integer?: boolean;
}

export interface BooleanField extends BaseField {
  type: "boolean";
}

export interface DateField extends BaseField {
  type: "date";
}

export interface ArrayField extends BaseField {
  type: "array";
  items?: Field;
}

export interface ObjectField extends BaseField {
  type: "object";
  properties?: Record<string, Field>;
}

export interface SetField extends BaseField {
  type: "set";
  itemType: "string" | "number";
}

export type Field =
  | StringField
  | NumberField
  | BooleanField
  | DateField
  | ArrayField
  | ObjectField
  | SetField;

export interface SchemaDefinition {
  [key: string]: Field;
}

export interface TableConfig {
  tableName: string;
  partitionKey: string;
  sortKey?: string;
  indexes?: {
    [indexName: string]: {
      partitionKey: string;
      sortKey?: string;
      type: "GSI" | "LSI";
    };
  };
}

// Convert schema to TypeScript types
export type InferFieldType<T extends Field> = T extends StringField
  ? string
  : T extends NumberField
  ? number
  : T extends BooleanField
  ? boolean
  : T extends DateField
  ? Date
  : T extends ArrayField
  ? Array<T["items"] extends Field ? InferFieldType<T["items"]> : any>
  : T extends ObjectField
  ? T["properties"] extends Record<string, Field>
    ? InferSchemaType<T["properties"]>
    : object
  : T extends SetField
  ? T["itemType"] extends "string"
    ? Set<string>
    : T["itemType"] extends "number"
    ? Set<number>
    : Set<any>
  : any;

export type InferSchemaType<T extends SchemaDefinition> = {
  [K in RequiredKeys<T>]: InferFieldType<NonNullable<T[K]>>;
} & {
  [K in OptionalKeys<T>]?: InferFieldType<NonNullable<T[K]>>;
};

// Query builder types
export interface WhereCondition {
  field: string;
  operator:
    | "="
    | "<"
    | "<="
    | ">"
    | ">="
    | "begins_with"
    | "contains"
    | "between"
    | "in";
  value: any;
  value2?: any; // for 'between' operator
}

export interface QueryOptions {
  limit?: number;
  scanIndexForward?: boolean;
  exclusiveStartKey?: Record<string, any>;
  indexName?: string;
  select?: string[];
  filterExpression?: WhereCondition[];
}

export interface ScanOptions extends Omit<QueryOptions, "scanIndexForward"> {
  segment?: number;
  totalSegments?: number;
}
