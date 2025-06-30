import { WhereCondition } from "../types";

// Expression builder utilities
export function buildConditionExpression(conditions: WhereCondition[]): {
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
