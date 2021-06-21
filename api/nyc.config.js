module.exports = {
  "all": true,
  "include": [
    "dist/cjs/*",
    "src/*"
  ],
  "exclude": [
    "**/*.spec.[jt]s",
    "dist/**/*.d.ts"
  ],
  "reporter": [
    "text",
    "json"
  ]
}