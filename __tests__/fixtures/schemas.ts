import { SchemaDefinition } from '../../src/types';

export const userSchema: SchemaDefinition = {
  id: { type: 'string', required: true },
  name: { type: 'string', required: true },
  age: { type: 'number' },
  createdAt: { type: 'date', default: () => new Date() },
};
