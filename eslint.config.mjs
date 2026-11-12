import importPlugin from 'eslint-plugin-import';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';
import scality from '@scality/eslint-config-scality';
import { mocha } from '@scality/eslint-config-scality/testing';

export default defineConfig(
    ...scality,
    {
        plugins: {
            import: importPlugin,
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
    {
        files: [tseslint.globs.ts],
        extends: [tseslint.configs.recommended],
        rules: {
            '@typescript-eslint/no-require-imports': 'off',
        },
    },
    mocha(['tests/**/*.js']),
);
