import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  ScanCommand,
  BatchGetItemCommand,
  BatchWriteItemCommand,
  AttributeValue,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

// Type utilities
type NonNullable<T> = T extends null | undefined ? never : T;
type RequiredKeys<T> = {
  [K in keyof T]-?: {} extends Pick<T, K> ? never : K;
}[keyof T];
type OptionalKeys<T> = {
  [K in keyof T]-?: {} extends Pick<T, K> ? K : never;
}[keyof T];

// Schema definition types
type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "date"
  | "array"
  | "object"
  | "set";

interface BaseField {
  type: FieldType;
  required?: boolean;
  default?: any;
  validate?: (value: any) => boolean | string;
}

interface StringField extends BaseField {
  type: "string";
  minLength?: number;
  maxLength?: number;
  pattern?: RegExp;
}

interface NumberField extends BaseField {
  type: "number";
  min?: number;
  max?: number;
  integer?: boolean;
}

interface BooleanField extends BaseField {
  type: "boolean";
}

interface DateField extends BaseField {
  type: "date";
}

interface ArrayField extends BaseField {
  type: "array";
  items?: Field;
}

interface ObjectField extends BaseField {
  type: "object";
  properties?: Record<string, Field>;
}

interface SetField extends BaseField {
  type: "set";
  itemType: "string" | "number";
}

type Field =
  | StringField
  | NumberField
  | BooleanField
  | DateField
  | ArrayField
  | ObjectField
  | SetField;

interface SchemaDefinition {
  [key: string]: Field;
}

interface TableConfig {
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
type InferFieldType<T extends Field> = T extends StringField
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
  ? Set<T["itemType"] extends "string" ? string : number>
  : any;

type InferSchemaType<T extends SchemaDefinition> = {
  [K in RequiredKeys<T>]: InferFieldType<NonNullable<T[K]>>;
} & {
  [K in OptionalKeys<T>]?: InferFieldType<NonNullable<T[K]>>;
};

// Query builder types
interface WhereCondition {
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

interface QueryOptions {
  limit?: number;
  scanIndexForward?: boolean;
  exclusiveStartKey?: Record<string, any>;
  indexName?: string;
  select?: string[];
  filterExpression?: WhereCondition[];
}

interface ScanOptions extends Omit<QueryOptions, "scanIndexForward"> {
  segment?: number;
  totalSegments?: number;
}

// Validation utilities
class ValidationError extends Error {
  constructor(message: string, public field?: string) {
    super(message);
    this.name = "ValidationError";
  }
}

function validateField(value: any, field: Field, fieldName: string): void {
  // Check required
  if (field.required && (value === undefined || value === null)) {
    throw new ValidationError(`Field '${fieldName}' is required`);
  }

  if (value === undefined || value === null) return;

  // Type validation
  switch (field.type) {
    case "string":
      if (typeof value !== "string") {
        throw new ValidationError(`Field '${fieldName}' must be a string`);
      }
      const stringField = field as StringField;
      if (stringField.minLength && value.length < stringField.minLength) {
        throw new ValidationError(
          `Field '${fieldName}' must be at least ${stringField.minLength} characters`
        );
      }
      if (stringField.maxLength && value.length > stringField.maxLength) {
        throw new ValidationError(
          `Field '${fieldName}' must be at most ${stringField.maxLength} characters`
        );
      }
      if (stringField.pattern && !stringField.pattern.test(value)) {
        throw new ValidationError(
          `Field '${fieldName}' does not match required pattern`
        );
      }
      break;

    case "number":
      if (typeof value !== "number" || isNaN(value)) {
        throw new ValidationError(
          `Field '${fieldName}' must be a valid number`
        );
      }
      const numberField = field as NumberField;
      if (numberField.integer && !Number.isInteger(value)) {
        throw new ValidationError(`Field '${fieldName}' must be an integer`);
      }
      if (numberField.min !== undefined && value < numberField.min) {
        throw new ValidationError(
          `Field '${fieldName}' must be at least ${numberField.min}`
        );
      }
      if (numberField.max !== undefined && value > numberField.max) {
        throw new ValidationError(
          `Field '${fieldName}' must be at most ${numberField.max}`
        );
      }
      break;

    case "boolean":
      if (typeof value !== "boolean") {
        throw new ValidationError(`Field '${fieldName}' must be a boolean`);
      }
      break;

    case "date":
      if (!(value instanceof Date) || isNaN(value.getTime())) {
        throw new ValidationError(`Field '${fieldName}' must be a valid Date`);
      }
      break;

    case "array":
      if (!Array.isArray(value)) {
        throw new ValidationError(`Field '${fieldName}' must be an array`);
      }
      break;

    case "object":
      if (typeof value !== "object" || Array.isArray(value)) {
        throw new ValidationError(`Field '${fieldName}' must be an object`);
      }
      break;

    case "set":
      if (!(value instanceof Set)) {
        throw new ValidationError(`Field '${fieldName}' must be a Set`);
      }
      break;
  }

  // Custom validation
  if (field.validate) {
    const result = field.validate(value);
    if (result !== true) {
      throw new ValidationError(
        typeof result === "string"
          ? result
          : `Field '${fieldName}' failed validation`
      );
    }
  }
}

function validateSchema<T extends SchemaDefinition>(
  data: any,
  schema: T
): void {
  for (const fieldName in schema) {
    if (schema.hasOwnProperty(fieldName)) {
      validateField(data[fieldName], schema[fieldName], fieldName);
    }
  }
}

// Expression builder utilities
function buildConditionExpression(conditions: WhereCondition[]): {
  expression: string;
  attributeNames: Record<string, string>;
  attributeValues: Record<string, any>;
} {
  const expressions: string[] = [];
  const attributeNames: Record<string, string> = {};
  const attributeValues: Record<string, any> = {};

  conditions.forEach((condition, index) => {
    const nameKey = `#field${index}`;
    const valueKey = `:value${index}`;

    attributeNames[nameKey] = condition.field;

    switch (condition.operator) {
      case "=":
        expressions.push(`${nameKey} = ${valueKey}`);
        attributeValues[valueKey] = condition.value;
        break;
      case "<":
        expressions.push(`${nameKey} < ${valueKey}`);
        attributeValues[valueKey] = condition.value;
        break;
      case "<=":
        expressions.push(`${nameKey} <= ${valueKey}`);
        attributeValues[valueKey] = condition.value;
        break;
      case ">":
        expressions.push(`${nameKey} > ${valueKey}`);
        attributeValues[valueKey] = condition.value;
        break;
      case ">=":
        expressions.push(`${nameKey} >= ${valueKey}`);
        attributeValues[valueKey] = condition.value;
        break;
      case "begins_with":
        expressions.push(`begins_with(${nameKey}, ${valueKey})`);
        attributeValues[valueKey] = condition.value;
        break;
      case "contains":
        expressions.push(`contains(${nameKey}, ${valueKey})`);
        attributeValues[valueKey] = condition.value;
        break;
      case "between":
        const valueKey2 = `:value${index}_2`;
        expressions.push(`${nameKey} BETWEEN ${valueKey} AND ${valueKey2}`);
        attributeValues[valueKey] = condition.value;
        attributeValues[valueKey2] = condition.value2;
        break;
      case "in":
        const inValues = Array.isArray(condition.value)
          ? condition.value
          : [condition.value];
        const inKeys = inValues.map((_, i) => `:value${index}_${i}`);
        expressions.push(`${nameKey} IN (${inKeys.join(", ")})`);
        inValues.forEach((val, i) => {
          attributeValues[`:value${index}_${i}`] = val;
        });
        break;
    }
  });

  return {
    expression: expressions.join(" AND "),
    attributeNames,
    attributeValues,
  };
}

// Main ORM class
export class DynamoORM<T extends SchemaDefinition> {
  private client: DynamoDBClient;
  private schema: T;
  private config: TableConfig;

  constructor(client: DynamoDBClient, schema: T, config: TableConfig) {
    this.client = client;
    this.schema = schema;
    this.config = config;
  }

  // Type-safe create method
  async create(item: InferSchemaType<T>): Promise<InferSchemaType<T>> {
    // Apply defaults
    const processedItem = { ...item } as any;
    for (const fieldName in this.schema) {
      if (this.schema.hasOwnProperty(fieldName)) {
        const field = this.schema[fieldName];
        if (
          processedItem[fieldName] === undefined &&
          field.default !== undefined
        ) {
          processedItem[fieldName] =
            typeof field.default === "function"
              ? field.default()
              : field.default;
        }
      }
    }

    // Validate
    validateSchema(processedItem, this.schema);

    // Convert Sets to arrays for marshalling
    const marshalledItem = this.preprocessForMarshall(processedItem);

    const command = new PutItemCommand({
      TableName: this.config.tableName,
      Item: marshall(marshalledItem) as unknown as Record<
        string,
        AttributeValue
      >,
      ConditionExpression: `attribute_not_exists(${this.config.partitionKey})`,
    });

    await this.client.send(command);
    return processedItem as InferSchemaType<T>;
  }

  // Type-safe get method
  async get(
    partitionKey: any,
    sortKey?: any
  ): Promise<InferSchemaType<T> | null> {
    const key: Record<string, any> = {
      [this.config.partitionKey]: partitionKey,
    };

    if (this.config.sortKey && sortKey !== undefined) {
      key[this.config.sortKey] = sortKey;
    }

    const command = new GetItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
    });

    const result = await this.client.send(command);

    if (!result.Item) {
      return null;
    }

    const item = unmarshall(result.Item);
    return this.postprocessFromMarshall(item) as InferSchemaType<T>;
  }

  // Type-safe update method
  async update(
    partitionKey: any,
    sortKey: any,
    updates: Partial<InferSchemaType<T>>
  ): Promise<InferSchemaType<T>> {
    // Validate updates
    for (const fieldName in updates) {
      if (updates.hasOwnProperty(fieldName)) {
        const value = (updates as any)[fieldName];
        if (this.schema[fieldName] && value !== undefined) {
          validateField(value, this.schema[fieldName], fieldName);
        }
      }
    }

    const key: Record<string, any> = {
      [this.config.partitionKey]: partitionKey,
    };

    if (this.config.sortKey) {
      key[this.config.sortKey] = sortKey;
    }

    // Build update expression
    const updateExpressions: string[] = [];
    const attributeNames: Record<string, string> = {};
    const attributeValues: Record<string, any> = {};

    let index = 0;
    for (const field in updates) {
      if (updates.hasOwnProperty(field)) {
        const value = (updates as any)[field];
        const nameKey = `#field${index}`;
        const valueKey = `:value${index}`;

        attributeNames[nameKey] = field;
        attributeValues[valueKey] = value;
        updateExpressions.push(`${nameKey} = ${valueKey}`);
        index++;
      }
    }

    const processedValues = marshall(
      this.preprocessForMarshall(attributeValues)
    ) as unknown as Record<string, AttributeValue>;

    const command = new UpdateItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
      UpdateExpression: `SET ${updateExpressions.join(", ")}`,
      ExpressionAttributeNames: attributeNames,
      ExpressionAttributeValues: processedValues,
      ReturnValues: "ALL_NEW",
    });

    const result = await this.client.send(command);
    const item = unmarshall(result.Attributes!);
    return this.postprocessFromMarshall(item) as InferSchemaType<T>;
  }

  // Type-safe delete method
  async delete(partitionKey: any, sortKey?: any): Promise<void> {
    const key: Record<string, any> = {
      [this.config.partitionKey]: partitionKey,
    };

    if (this.config.sortKey && sortKey !== undefined) {
      key[this.config.sortKey] = sortKey;
    }

    const command = new DeleteItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
    });

    await this.client.send(command);
  }

  // Type-safe query method
  async query(
    partitionKeyValue: any,
    options: QueryOptions = {}
  ): Promise<{
    items: InferSchemaType<T>[];
    lastEvaluatedKey?: Record<string, any>;
    count: number;
  }> {
    const keyConditionExpression = `#pk = :pkValue`;
    const expressionAttributeNames: Record<string, string> = {
      "#pk": this.config.partitionKey,
    };
    const expressionAttributeValues: Record<string, any> = {
      ":pkValue": partitionKeyValue,
    };

    let filterExpression: string | undefined;

    if (options.filterExpression && options.filterExpression.length > 0) {
      const filterResult = buildConditionExpression(options.filterExpression);
      filterExpression = filterResult.expression;
      Object.assign(expressionAttributeNames, filterResult.attributeNames);
      Object.assign(expressionAttributeValues, filterResult.attributeValues);
    }

    let projectionExpression: string | undefined;
    if (options.select && options.select.length > 0) {
      projectionExpression = options.select
        .map((field, index) => {
          const nameKey = `#select${index}`;
          expressionAttributeNames[nameKey] = field;
          return nameKey;
        })
        .join(", ");
    }

    const command = new QueryCommand({
      TableName: this.config.tableName,
      IndexName: options.indexName,
      KeyConditionExpression: keyConditionExpression,
      FilterExpression: filterExpression,
      ProjectionExpression: projectionExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: marshall(
        this.preprocessForMarshall(expressionAttributeValues)
      ) as unknown as Record<string, AttributeValue>,
      Limit: options.limit,
      ScanIndexForward: options.scanIndexForward,
      ExclusiveStartKey: options.exclusiveStartKey
        ? (marshall(options.exclusiveStartKey) as unknown as Record<
            string,
            AttributeValue
          >)
        : undefined,
    });

    const result = await this.client.send(command);

    return {
      items: (result.Items || []).map(
        (item) =>
          this.postprocessFromMarshall(unmarshall(item)) as InferSchemaType<T>
      ),
      lastEvaluatedKey: result.LastEvaluatedKey
        ? unmarshall(result.LastEvaluatedKey)
        : undefined,
      count: result.Count || 0,
    };
  }

  // Type-safe scan method
  async scan(options: ScanOptions = {}): Promise<{
    items: InferSchemaType<T>[];
    lastEvaluatedKey?: Record<string, any>;
    count: number;
  }> {
    const expressionAttributeNames: Record<string, string> = {};
    const expressionAttributeValues: Record<string, any> = {};

    let filterExpression: string | undefined;

    if (options.filterExpression && options.filterExpression.length > 0) {
      const filterResult = buildConditionExpression(options.filterExpression);
      filterExpression = filterResult.expression;
      Object.assign(expressionAttributeNames, filterResult.attributeNames);
      Object.assign(expressionAttributeValues, filterResult.attributeValues);
    }

    let projectionExpression: string | undefined;
    if (options.select && options.select.length > 0) {
      projectionExpression = options.select
        .map((field, index) => {
          const nameKey = `#select${index}`;
          expressionAttributeNames[nameKey] = field;
          return nameKey;
        })
        .join(", ");
    }

    const command = new ScanCommand({
      TableName: this.config.tableName,
      IndexName: options.indexName,
      FilterExpression: filterExpression,
      ProjectionExpression: projectionExpression,
      ExpressionAttributeNames:
        Object.keys(expressionAttributeNames).length > 0
          ? expressionAttributeNames
          : undefined,
      ExpressionAttributeValues:
        Object.keys(expressionAttributeValues).length > 0
          ? (marshall(
              this.preprocessForMarshall(expressionAttributeValues)
            ) as unknown as Record<string, AttributeValue>)
          : undefined,
      Limit: options.limit,
      ExclusiveStartKey: options.exclusiveStartKey
        ? (marshall(options.exclusiveStartKey) as unknown as Record<
            string,
            AttributeValue
          >)
        : undefined,
      Segment: options.segment,
      TotalSegments: options.totalSegments,
    });

    const result = await this.client.send(command);

    return {
      items: (result.Items || []).map(
        (item) =>
          this.postprocessFromMarshall(unmarshall(item)) as InferSchemaType<T>
      ),
      lastEvaluatedKey: result.LastEvaluatedKey
        ? unmarshall(result.LastEvaluatedKey)
        : undefined,
      count: result.Count || 0,
    };
  }

  // Batch operations
  async batchGet(
    keys: Array<{ partitionKey: any; sortKey?: any }>
  ): Promise<InferSchemaType<T>[]> {
    const requestItems = keys.map((key) => {
      const keyObj: Record<string, any> = {
        [this.config.partitionKey]: key.partitionKey,
      };
      if (this.config.sortKey && key.sortKey !== undefined) {
        keyObj[this.config.sortKey] = key.sortKey;
      }
      return marshall(keyObj) as unknown as Record<string, AttributeValue>;
    });

    const command = new BatchGetItemCommand({
      RequestItems: {
        [this.config.tableName]: {
          Keys: requestItems,
        },
      },
    });

    const result = await this.client.send(command);
    const items = result.Responses?.[this.config.tableName] || [];

    return items.map(
      (item) =>
        this.postprocessFromMarshall(unmarshall(item)) as InferSchemaType<T>
    );
  }

  async batchWrite(
    operations: Array<{
      operation: "put" | "delete";
      item?: InferSchemaType<T>;
      key?: { partitionKey: any; sortKey?: any };
    }>
  ): Promise<void> {
    const writeRequests = operations.map((op) => {
      if (op.operation === "put" && op.item) {
        validateSchema(op.item, this.schema);
        return {
          PutRequest: {
            Item: marshall(
              this.preprocessForMarshall(op.item)
            ) as unknown as Record<string, AttributeValue>,
          },
        };
      } else if (op.operation === "delete" && op.key) {
        const keyObj: Record<string, any> = {
          [this.config.partitionKey]: op.key.partitionKey,
        };
        if (this.config.sortKey && op.key.sortKey !== undefined) {
          keyObj[this.config.sortKey] = op.key.sortKey;
        }
        return {
          DeleteRequest: {
            Key: marshall(keyObj) as unknown as Record<string, AttributeValue>,
          },
        };
      }
      throw new Error("Invalid batch operation");
    });

    const command = new BatchWriteItemCommand({
      RequestItems: {
        [this.config.tableName]: writeRequests,
      },
    });

    await this.client.send(command);
  }

  // Helper methods for Set handling
  private preprocessForMarshall(item: any): any {
    if (item === null || item === undefined) return item;

    if (item instanceof Set) {
      return Array.from(item);
    }

    if (Array.isArray(item)) {
      return item.map((i) => this.preprocessForMarshall(i));
    }

    if (typeof item === "object" && item instanceof Date) {
      return item.toISOString();
    }

    if (typeof item === "object") {
      const result: any = {};
      for (const key in item) {
        if (item.hasOwnProperty(key)) {
          result[key] = this.preprocessForMarshall(item[key]);
        }
      }
      return result;
    }

    return item;
  }

  private postprocessFromMarshall(item: any): any {
    if (item === null || item === undefined) return item;

    if (Array.isArray(item)) {
      return item.map((i) => this.postprocessFromMarshall(i));
    }

    if (typeof item === "object") {
      const result: any = {};
      for (const key in item) {
        if (item.hasOwnProperty(key)) {
          const value = item[key];
          const fieldDef = this.schema[key];
          if (fieldDef?.type === "set" && Array.isArray(value)) {
            result[key] = new Set(value);
          } else if (fieldDef?.type === "date" && typeof value === "string") {
            result[key] = new Date(value);
          } else {
            result[key] = this.postprocessFromMarshall(value);
          }
        }
      }
      return result;
    }

    return item;
  }
}

// Helper function to create schema fields
export const field = {
  string: (options: Omit<StringField, "type"> = {}): StringField => ({
    type: "string",
    ...options,
  }),

  number: (options: Omit<NumberField, "type"> = {}): NumberField => ({
    type: "number",
    ...options,
  }),

  boolean: (options: Omit<BooleanField, "type"> = {}): BooleanField => ({
    type: "boolean",
    ...options,
  }),

  date: (options: Omit<DateField, "type"> = {}): DateField => ({
    type: "date",
    ...options,
  }),

  array: (options: Omit<ArrayField, "type"> = {}): ArrayField => ({
    type: "array",
    ...options,
  }),

  object: (options: Omit<ObjectField, "type"> = {}): ObjectField => ({
    type: "object",
    ...options,
  }),

  stringSet: (options: Omit<SetField, "type" | "itemType"> = {}): SetField => ({
    type: "set",
    itemType: "string",
    ...options,
  }),

  numberSet: (options: Omit<SetField, "type" | "itemType"> = {}): SetField => ({
    type: "set",
    itemType: "number",
    ...options,
  }),
};

// Export types for external use
export type {
  SchemaDefinition,
  TableConfig,
  Field,
  InferSchemaType,
  WhereCondition,
  QueryOptions,
  ScanOptions,
  ValidationError,
};

// Usage example:
/*
  import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
  import { DynamoORM, field } from './dynamo-orm';
  
  // Define your schema
  const userSchema = {
    id: field.string({ required: true }),
    email: field.string({ 
      required: true, 
      pattern: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
      validate: (email) => email.includes('@') || 'Email must contain @'
    }),
    name: field.string({ required: true, minLength: 2, maxLength: 100 }),
    age: field.number({ min: 0, max: 150, integer: true }),
    isActive: field.boolean({ default: true }),
    createdAt: field.date({ default: () => new Date() }),
    tags: field.stringSet(),
    metadata: field.object(),
    scores: field.array({ items: field.number() })
  } as const;
  
  // Create ORM instance
  const client = new DynamoDBClient({ region: 'us-east-1' });
  const userORM = new DynamoORM(client, userSchema, {
    tableName: 'users',
    partitionKey: 'id',
    indexes: {
      'email-index': {
        partitionKey: 'email',
        type: 'GSI'
      }
    }
  });
  
  // Type-safe operations
  async function example() {
    // Create - fully type-safe
    const user = await userORM.create({
      id: '123',
      email: 'john@example.com',
      name: 'John Doe',
      age: 30,
      tags: new Set(['developer', 'typescript']),
      scores: [95, 87, 92]
    });
  
    // Get - returns typed result or null
    const foundUser = await userORM.get('123');
    if (foundUser) {
      console.log(foundUser.name); // TypeScript knows this is string
    }
  
    // Update - partial updates with validation
    const updatedUser = await userORM.update('123', undefined, {
      age: 31,
      isActive: false
    });
  
    // Query with type-safe filters
    const results = await userORM.query('123', {
      filterExpression: [
        { field: 'age', operator: '>', value: 18 },
        { field: 'isActive', operator: '=', value: true }
      ],
      limit: 10
    });
  
    // Batch operations
    await userORM.batchWrite([
      {
        operation: 'put',
        item: {
          id: '456',
          email: 'jane@example.com',
          name: 'Jane Doe',
          age: 25
        }
      },
      {
        operation: 'delete',
        key: { partitionKey: '789' }
      }
    ]);
  }
  */
