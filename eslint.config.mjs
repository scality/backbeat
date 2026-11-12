import mocha from 'eslint-plugin-mocha';
import importPlugin from 'eslint-plugin-import';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';
import globals from 'globals';
import scality from '@scality/eslint-config-scality';

export default defineConfig(
    ...scality,
    {
        plugins: {
            mocha,
            import: importPlugin,
        },

        rules: {
            'mocha/no-exclusive-tests': 'error',
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
    {
        files: [tseslint.globs.ts],
        extends: [tseslint.configs.recommended],
        rules: {
            '@typescript-eslint/no-require-imports': 'off',
        },
    },
    {
        files: ['tests/**/*.js'],

        languageOptions: {
            globals: globals.mocha,
        },
    },
);
