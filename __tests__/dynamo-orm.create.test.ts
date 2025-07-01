import { mockClient } from 'aws-sdk-client-mock';
import {
  DynamoDBClient,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

import { DynamoORM } from '../src/orm';
import { userSchema } from './fixtures/schemas';
import { TableConfig } from '../src/types';

// Create a mock for DynamoDBClient that can intercept command calls
const ddbMock = mockClient(DynamoDBClient);

const tableConfig: TableConfig = {
  tableName: 'Users',
  partitionKey: 'id',
};

beforeEach(() => {
  ddbMock.reset();
});

describe('DynamoORM.create()', () => {
  it('should send PutItemCommand and return the original item', async () => {
    // Arrange
    const input = { id: '123', name: 'Ada' } as const;
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
    expect(sent.TableName).toBe('Users');
    //  Item should at least contain the partition key
    expect(sent.Item).toMatchObject(marshall(input));
  });
});
