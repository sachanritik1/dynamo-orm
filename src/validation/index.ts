import { Field, SchemaDefinition, StringField, NumberField } from "../types";

// Validation utilities
export class ValidationError extends Error {
  constructor(message: string, public field?: string) {
    super(message);
    this.name = "ValidationError";
  }
}

export function validateField(value: any, field: Field, fieldName: string): void {
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

export function validateSchema<T extends SchemaDefinition>(
  data: any,
  schema: T
): void {
  for (const fieldName in schema) {
    if (schema.hasOwnProperty(fieldName)) {
      validateField(data[fieldName], schema[fieldName], fieldName);
    }
  }
}
