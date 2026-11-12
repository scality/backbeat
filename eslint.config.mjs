import importPlugin from 'eslint-plugin-import';
import tseslint from 'typescript-eslint';
import scality from '@scality/eslint-config-scality';
import { mocha } from '@scality/eslint-config-scality/testing';

export default [
    ...scality,
    {
        plugins: {
            import: importPlugin,
        },

        languageOptions: {
            ecmaVersion: 2020,
        },

        rules: {
            // no-extraneous-dependencies silently skips modules it cannot resolve
            'import/no-unresolved': ['error', { commonjs: true }],
            'import/no-extraneous-dependencies': [
                'error',
                {
                    devDependencies: ['tests/**', '**/*.config.*', '.github/**'],
                },
            ],
        },

        settings: {
            'import/resolver': {
                node: {
                    paths: ['/backbeat/node_modules', 'node_modules'],
                    extensions: ['.js', '.ts', '.json'],
                },
            },
        },
    },
    {
        files: ['**/*.js'],
        languageOptions: {
            sourceType: 'commonjs',
        },
    },
    ...tseslint.configs.recommended.map(config => ({
        ...config,
        files: ['**/*.ts'],
    })),
    {
        files: ['**/*.ts'],

        rules: {
            '@typescript-eslint/no-require-imports': 'off',
        },
    },
    mocha(['tests/**/*.js']),
];
