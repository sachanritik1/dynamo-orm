import { field } from "../../src";

export const userSchema = {
  id: field.string({ required: true }),
  name: field.string({ required: true }),
  age: field.number(),
  createdAt: field.date({ default: () => new Date() }),
  updatedAt: field.date({ default: () => new Date() }),
};
