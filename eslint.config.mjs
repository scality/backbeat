import mocha from "eslint-plugin-mocha";
import importPlugin from "eslint-plugin-import";
import tseslint from "typescript-eslint";
import globals from "globals";
import scality from "@scality/eslint-config-scality";

export default [...scality, {
    plugins: {
        mocha,
        import: importPlugin,
    },

    languageOptions: {
        ecmaVersion: 2020,
        sourceType: "script",
    },

    rules: {
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
}, {
    files: ["tests/**/*.js"],

    languageOptions: {
        globals: globals.mocha,
    },
}];
