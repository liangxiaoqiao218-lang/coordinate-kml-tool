export function finiteNumberOrNull(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function hasFiniteNumericValue(value) {
  return finiteNumberOrNull(value) !== null;
}
