import { mockClient } from "aws-sdk-client-mock";
import {
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";

import { DynamoORM } from "../src/orm";
import { userSchema } from "./fixtures/schemas";
import { TableConfig } from "../src/types";

// Create a mock for DynamoDBClient that can intercept command calls
const ddbMock = mockClient(DynamoDBClient);

const tableConfig: TableConfig = {
  tableName: "Users",
  partitionKey: "id",
};

beforeEach(() => {
  ddbMock.reset();
});

describe("DynamoORM.create()", () => {
  it("should send PutItemCommand and return the original item", async () => {
    // Arrange
    const input = { id: "123", name: "Ada" };
    ddbMock.on(PutItemCommand).resolves({});

    const orm = new DynamoORM(new DynamoDBClient({}), userSchema, tableConfig);

    // Act
    const result = await orm.create({ ...input });

    // Assert
    expect(result).toEqual(expect.objectContaining(input));
    // Ensure exactly one PutItemCommand was sent
    expect(ddbMock.commandCalls(PutItemCommand).length).toBe(1);

    // Inspect the command payload
    const sent = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(sent.TableName).toBe("Users");
    //  Item should at least contain the partition key
    expect(sent.Item).toMatchObject(marshall(input));
  });
  it("should throw an error if the item is invalid", async () => {
    // Arrange
    const input = { id: "123", name: "Ada" };
    ddbMock.on(PutItemCommand).resolves({});

    const orm = new DynamoORM(new DynamoDBClient({}), userSchema, tableConfig);

    // Act
    const result = await orm.create({ ...input });

    // Assert
    expect(result).toEqual(expect.objectContaining(input));
    // Ensure exactly one PutItemCommand was sent
    expect(ddbMock.commandCalls(PutItemCommand).length).toBe(1);

    // Inspect the command payload
    const sent = ddbMock.commandCalls(PutItemCommand)[0].args[0].input;
    expect(sent.TableName).toBe("Users");
    //  Item should at least contain the partition key
    expect(sent.Item).toMatchObject(marshall(input));
  });

  it("should get item by key", async () => {
    // Arrange
    const input = { id: "123", name: "Ada" };
    ddbMock.on(GetItemCommand).resolves({ Item: marshall(input) });

    const orm = new DynamoORM(new DynamoDBClient({}), userSchema, tableConfig);

    // Act
    const result = await orm.get({ id: "123" });

    // Assert
    expect(result).toEqual(expect.objectContaining(input));
    // Ensure exactly one GetItemCommand was sent
    expect(ddbMock.commandCalls(GetItemCommand).length).toBe(1);

    // Inspect the command payload
    const sent = ddbMock.commandCalls(GetItemCommand)[0].args[0].input;
    expect(sent.TableName).toBe("Users");
    //  Key should at least contain the partition key
    expect(sent.Key).toMatchObject(marshall({ id: "123" }));
  });

  it("should update item by key", async () => {
    // Arrange
    const input = { id: "123", name: "Ada" };
    const updatedInput = { ...input, name: "Adam" };
    ddbMock
      .on(UpdateItemCommand)
      .resolves({ Attributes: marshall(updatedInput) });

    const orm = new DynamoORM(new DynamoDBClient({}), userSchema, tableConfig);

    // Act
    const result = await orm.update({ id: "123" }, { name: "Adam" });

    // Assert
    expect(result).toEqual(expect.objectContaining(updatedInput));
    // Ensure exactly one UpdateItemCommand was sent
    expect(ddbMock.commandCalls(UpdateItemCommand).length).toBe(1);

    // Inspect the command payload
    const sent = ddbMock.commandCalls(UpdateItemCommand)[0].args[0].input;
    expect(sent.TableName).toBe("Users");
  });

  it("should delete item by key", async () => {
    // Arrange
    const input = { id: "123", name: "Ada" };
    ddbMock.on(DeleteItemCommand).resolves({});

    const orm = new DynamoORM(new DynamoDBClient({}), userSchema, tableConfig);

    // Act
    const result = await orm.delete({ id: "123" });

    // Assert
    expect(result).toBeUndefined();
    // Ensure exactly one DeleteItemCommand was sent
    expect(ddbMock.commandCalls(DeleteItemCommand).length).toBe(1);
  });
});
