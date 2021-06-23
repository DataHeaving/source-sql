module.exports = {
  "all": true,
  "include": [
    "dist/cjs/*",
    "src/*"
  ],
  "exclude": [
    "**/*.spec.[jt]s",
    "dist/**/*.d.ts",
    "**/tests-setup/*.[jt]s"
  ],
  "reporter": [
    "text",
    "json"
  ]
}