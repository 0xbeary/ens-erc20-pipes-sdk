/**
 * Converts a camelCase or PascalCase string to snake_case
 * @param str - The string to convert
 * @returns The snake_case version of the string
 */
export function toSnakeCase(str: string): string {
  return str
    .replace(/([A-Z])/g, '_$1')
    .toLowerCase()
    .replace(/^_/, '')
}
