// Usage example:

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoORM, field } from "./index";

// Define your schema
const userSchema = {
  id: field.string({ required: true }),
  email: field.string({ required: true }),
  name: field.string({ required: true, minLength: 2 }),
  age: field.number(),
  isActive: field.boolean({ default: true }),
  createdAt: field.date({ default: () => new Date() }),
  tags: field.stringSet(),
  metadata: field.object(),
  scores: field.array({ items: field.number() }),
};

// Create ORM instance
const client = new DynamoDBClient({ region: "us-east-1" });
const userORM = new DynamoORM(client, userSchema, {
  tableName: "users",
  partitionKey: "id",
  indexes: {
    "email-index": {
      partitionKey: "email",
      type: "GSI",
    },
  },
});

// Type-safe operations
async function example() {
  // Create - fully type-safe
  const user = await userORM.create({
    id: "123",
    email: "john@example.com",
    name: "John Doe",
    age: 30,
    tags: new Set(["developer", "typescript"]),
    scores: [95, 87, 92],
  });

  console.log(user);

  // Get - returns typed result or null
  const foundUser = await userORM.get("123");
  if (foundUser) {
    console.log(foundUser.name); // TypeScript knows this is string
  }

  // Update - partial updates with validation
  const updatedUser = await userORM.update("123", undefined, {
    age: 31,
    isActive: false,
  });

  // Query with type-safe filters
  const results = await userORM.query("123", {
    filterExpression: [
      { field: "age", operator: ">", value: 18 },
      { field: "isActive", operator: "=", value: true },
    ],
    limit: 10,
  });

  // Batch operations
  await userORM.batchWrite([
    {
      operation: "put",
      item: {
        id: "456",
        email: "jane@example.com",
        name: "Jane Doe",
        age: 25,
        tags: new Set(["user", "premium"]),
        scores: [85, 90, 78],
      },
    },
    {
      operation: "delete",
      key: { partitionKey: "789" },
    },
  ]);
}
