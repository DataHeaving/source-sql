module.exports = {
  env: {
    node: true,
    es2020: true
  },
  parserOptions: {
    project: "./tsconfig.json",
    sourceType: "module",
    ecmaVersion: 2020,
  },
  rules: {
    "prettier/prettier": "error",
    //"function-paren-newline": ["error", "always"],
    "@typescript-eslint/explicit-module-boundary-types": "off", // IDE will show the return types
    "@typescript-eslint/restrict-template-expressions": "off", // We are OK with whatever type within template expressions
    "@typescript-eslint/unbound-method": "off", // We never use 'this' within functions anyways.
    "@typescript-eslint/no-empty-function": "off", // Empty functions are ok sometimes.
    "no-useless-return": "error",
    "no-console": "error"
  },
  settings: {
    "import/resolver": {
      node: {
        paths: ["src"],
        extensions: [".ts"] // Add .tsx, .js, .jsx if needed
      }
    }
  }
};
