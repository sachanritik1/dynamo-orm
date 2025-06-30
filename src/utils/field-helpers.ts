import {
    StringField,
    NumberField,
    BooleanField,
    DateField,
    ArrayField,
    ObjectField,
    SetField,
  } from "../types";
  
  // Helper function to create schema fields
  export const field = {
    // Each helper is generic so that literal option values (like `required: true`) are preserved
    string<O extends Omit<StringField, "type">>(options = {} as O) {
      return { type: "string" as const, ...options };
    },
    number<O extends Omit<NumberField, "type">>(options = {} as O) {
      return { type: "number" as const, ...options };
    },
    boolean<O extends Omit<BooleanField, "type">>(options = {} as O) {
      return { type: "boolean" as const, ...options };
    },
    date<O extends Omit<DateField, "type">>(options = {} as O) {
      return { type: "date" as const, ...options };
    },
    array<O extends Omit<ArrayField, "type">>(options = {} as O) {
      return { type: "array" as const, ...options };
    },
    object<O extends Omit<ObjectField, "type">>(options = {} as O) {
      return { type: "object" as const, ...options };
    },
    stringSet<O extends Omit<SetField, "type" | "itemType">>(
      options = {} as O
    ) {
      return { type: "set" as const, itemType: "string" as const, ...options };
    },
    numberSet<O extends Omit<SetField, "type" | "itemType">>(
      options = {} as O
    ) {
      return { type: "set" as const, itemType: "number" as const, ...options };
    },
  };
