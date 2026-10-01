import mocha from "eslint-plugin-mocha";
import importPlugin from "eslint-plugin-import";
import path from "node:path";
import { fileURLToPath } from "node:url";
import js from "@eslint/js";
import { FlatCompat } from "@eslint/eslintrc";
import tseslint from "typescript-eslint";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});

export default [...compat.extends("@scality/eslint-config-scality"), {
    plugins: {
        mocha,
        import: importPlugin,
    },

    languageOptions: {
        ecmaVersion: 2020,
        sourceType: "script",
    },

    rules: {
        "object-curly-newline": "off",
        "import/newline-after-import": "off",
        "import/order": "off",
        "prefer-destructuring": "off",
        "operator-linebreak": "off",
        "no-underscore-dangle": "off",
        "indent": "off",
        "function-paren-newline": "off",
        "implicit-arrow-linebreak": "off",
        "no-bitwise": "off",
        "comma-dangle": "off",
        "padded-blocks": "off",
        "lines-around-directive": "off",
        "global-require": "off",
        "import/no-dynamic-require": "off",
        "object-property-newline": "off",
        "no-plusplus": "off",
        "class-methods-use-this": "off",
        "no-lonely-if": "off",
        "no-else-return": "off",
        "dot-location": "off",
        "no-restricted-properties": "off",
        "no-buffer-constructor": "off",
        "no-restricted-globals": "off",
        "no-useless-return": "off",
        "no-multi-spaces": "off",
        "space-unary-ops": "off",
        "no-undef-init": "off",
        "newline-per-chained-call": "off",
        "no-useless-escape": "off",
        "no-redeclare":"off",
        "mocha/no-exclusive-tests": "error",
        // no-extraneous-dependencies silently skips modules it cannot resolve
        "import/no-unresolved": ["error", { commonjs: true }],
        "import/no-extraneous-dependencies": ["error", {
            devDependencies: ["tests/**", "**/*.config.*", ".github/**"],
        }],
    },

    settings: {
        'import/resolver': {
            node: {
                paths: ["/backbeat/node_modules", "node_modules"],
                extensions: [".js", ".ts", ".json"],
            }
        }
    },
}, ...tseslint.configs.recommended.map(config => ({
    ...config,
    files: ["**/*.ts"],
})), {
    files: ["**/*.ts"],

    rules: {
        "@typescript-eslint/no-require-imports": "off",
    },
}];
