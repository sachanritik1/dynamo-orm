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
import {
  SchemaDefinition,
  TableConfig,
  InferSchemaType,
  QueryOptions,
  ScanOptions,
  WhereCondition,
} from "../types";
import { validateSchema } from "../validation";
import { buildConditionExpression } from "../utils/expression-builder";

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
      Item: marshall(marshalledItem, {
        removeUndefinedValues: true,
      }) as unknown as Record<string, AttributeValue>,
    });

    await this.client.send(command);
    return processedItem as InferSchemaType<T>;
  }

  // Type-safe get method
  async get(key: Record<string, any>): Promise<InferSchemaType<T> | null> {
    const command = new GetItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
    });

    const { Item } = await this.client.send(command);

    if (!Item) {
      return null;
    }

    return this.postprocessFromMarshall(unmarshall(Item)) as InferSchemaType<T>;
  }

  // Type-safe update method
  async update(
    key: Record<string, any>,
    data: Partial<InferSchemaType<T>>
  ): Promise<InferSchemaType<T>> {
    const updateExpression: string[] = [];
    const expressionAttributeNames: Record<string, string> = {};
    const expressionAttributeValues: Record<string, any> = {};

    let i = 0;
    for (const fieldName in data) {
      if (data.hasOwnProperty(fieldName)) {
        const nameKey = `#field${i}`;
        const valueKey = `:value${i}`;

        updateExpression.push(`${nameKey} = ${valueKey}`);
        expressionAttributeNames[nameKey] = fieldName;
        expressionAttributeValues[valueKey] = (data as any)[fieldName];
        i++;
      }
    }

    const command = new UpdateItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
      UpdateExpression: `SET ${updateExpression.join(", ")}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: marshall(
        this.preprocessForMarshall(expressionAttributeValues),
        { removeUndefinedValues: true }
      ) as unknown as Record<string, AttributeValue>,
      ReturnValues: "ALL_NEW",
    });

    const { Attributes } = await this.client.send(command);

    return this.postprocessFromMarshall(
      unmarshall(Attributes || {})
    ) as InferSchemaType<T>;
  }

  // Type-safe delete method
  async delete(key: Record<string, any>): Promise<void> {
    const command = new DeleteItemCommand({
      TableName: this.config.tableName,
      Key: marshall(key) as unknown as Record<string, AttributeValue>,
    });

    await this.client.send(command);
  }

  // Type-safe query method
  async query(
    conditions: WhereCondition[],
    options: QueryOptions = {}
  ): Promise<{ items: InferSchemaType<T>[]; lastEvaluatedKey?: any }> {
    const { expression, attributeNames, attributeValues } =
      buildConditionExpression(conditions);

    const marshalledValues = marshall(
      this.preprocessForMarshall(attributeValues),
      { removeUndefinedValues: true }
    );

    const command = new QueryCommand({
      TableName: this.config.tableName,
      KeyConditionExpression: expression,
      ExpressionAttributeNames: attributeNames,
      ExpressionAttributeValues: Object.keys(marshalledValues).length
        ? (marshalledValues as unknown as Record<string, AttributeValue>)
        : undefined,
      Limit: options.limit,
      ScanIndexForward: options.scanIndexForward,
      ExclusiveStartKey: options.exclusiveStartKey
        ? (marshall(options.exclusiveStartKey) as unknown as Record<
            string,
            AttributeValue
          >)
        : undefined,
      IndexName: options.indexName,
    });

    const { Items, LastEvaluatedKey } = await this.client.send(command);

    const items =
      Items?.map((item) => this.postprocessFromMarshall(unmarshall(item))) ||
      [];

    return {
      items: items as InferSchemaType<T>[],
      lastEvaluatedKey: LastEvaluatedKey
        ? unmarshall(LastEvaluatedKey)
        : undefined,
    };
  }

  // Type-safe scan method
  async scan(
    options: ScanOptions = {}
  ): Promise<{ items: InferSchemaType<T>[]; lastEvaluatedKey?: any }> {
    const command = new ScanCommand({
      TableName: this.config.tableName,
      Limit: options.limit,
      ExclusiveStartKey: options.exclusiveStartKey
        ? (marshall(options.exclusiveStartKey) as unknown as Record<
            string,
            AttributeValue
          >)
        : undefined,
      IndexName: options.indexName,
      Segment: options.segment,
      TotalSegments: options.totalSegments,
    });

    const { Items, LastEvaluatedKey } = await this.client.send(command);

    const items =
      Items?.map((item) => this.postprocessFromMarshall(unmarshall(item))) ||
      [];

    return {
      items: items as InferSchemaType<T>[],
      lastEvaluatedKey: LastEvaluatedKey
        ? unmarshall(LastEvaluatedKey)
        : undefined,
    };
  }

  // Batch get
  async batchGet(keys: Record<string, any>[]): Promise<InferSchemaType<T>[]> {
    const command = new BatchGetItemCommand({
      RequestItems: {
        [this.config.tableName]: {
          Keys: keys.map(
            (key) => marshall(key) as unknown as Record<string, AttributeValue>
          ),
        },
      },
    });

    const { Responses } = await this.client.send(command);
    const items = Responses?.[this.config.tableName] || [];

    return items.map((item) =>
      this.postprocessFromMarshall(unmarshall(item))
    ) as InferSchemaType<T>[];
  }

  // Batch write (put/delete)
  async batchWrite(
    requests: (
      | { type: "put"; item: InferSchemaType<T> }
      | { type: "delete"; key: Record<string, any> }
    )[]
  ): Promise<void> {
    const writeRequests = requests.map((req) => {
      if (req.type === "put") {
        return {
          PutRequest: {
            Item: marshall(this.preprocessForMarshall(req.item), {
              removeUndefinedValues: true,
            }) as unknown as Record<string, AttributeValue>,
          },
        };
      }
      return {
        DeleteRequest: {
          Key: marshall(req.key) as unknown as Record<string, AttributeValue>,
        },
      };
    });

    const command = new BatchWriteItemCommand({
      RequestItems: {
        [this.config.tableName]: writeRequests,
      },
    });

    await this.client.send(command);
  }

  private preprocessForMarshall(item: any): any {
    if (item === null || typeof item !== "object") {
      return item;
    }

    if (item instanceof Set) {
      return Array.from(item);
    }

    if (item instanceof Date) {
      return item.toISOString();
    }

    if (Array.isArray(item)) {
      return item.map((i) => this.preprocessForMarshall(i));
    }

    const result: any = {};
    for (const key in item) {
      if (item.hasOwnProperty(key)) {
        result[key] = this.preprocessForMarshall(item[key]);
      }
    }
    return result;
  }

  private postprocessFromMarshall(item: any): any {
    const newItem: { [key: string]: any } = {};

    for (const key in item) {
      const fieldDefinition = this.schema[key];
      const value = item[key];

      if (fieldDefinition) {
        if (fieldDefinition.type === "date" && typeof value === "string") {
          newItem[key] = new Date(value);
        } else if (
          fieldDefinition.type === "set" &&
          Array.isArray(value)
        ) {
          newItem[key] = new Set(value);
        } else {
          newItem[key] = value;
        }
      } else {
        newItem[key] = value;
      }
    }
    return newItem;
  }
}
