
class TestConfigurator {

    constructor() {
        this.persistentParams = {};
        this.testParams = {};
    }

    getParam(paramPath) {
        const testParam = this._lookupParam(paramPath, this.testParams);
        const persistentParam = this._lookupParam(paramPath,
                                                  this.persistentParams);
        if (testParam === undefined && persistentParam === undefined) {
            throw new Error(`error looking up param '${paramPath}': ` +
                            'not defined');
        }
        return this._mergeParams(testParam, persistentParam);
    }

    _lookupParam(paramPath, paramRepo) {
        let param = paramRepo;
        paramPath.split('.').forEach(child => {
            if (param !== undefined) {
                if (typeof param === 'function') {
                    param = param();
                }
                param = param[child];
            }
        });
        if (param === undefined) {
            return undefined;
        }
        if (typeof param === 'function') {
            const resolvedParam = param();
            return resolvedParam;
        }
        if (typeof param === 'object' && !Array.isArray(param)) {
            const resolvedParam = {};
            Object.keys(param).forEach(key => {
                resolvedParam[key] = this.getParam(`${paramPath}.${key}`);
            });
            return resolvedParam;
        }
        return param;
    }

    _mergeParams(testParam, persistentParam) {
        if (testParam === undefined) {
            return persistentParam;
        }
        if (typeof testParam !== 'object' || Array.isArray(testParam) ||
            typeof persistentParam !== 'object' ||
            Array.isArray(persistentParam)) {
            // prioritize test param
            return testParam;
        }
        const mergedObj = Object.assign({}, persistentParam);
        Object.keys(testParam).forEach(key => {
            mergedObj[key] = this._mergeParams(testParam[key],
                                               persistentParam[key]);
        });
        return mergedObj;
    }

    setParam(paramPath, paramValue, options) {
        function _wrapStatic(param) {
            if (typeof param === 'function') {
                // params are dynamic by default, wrap functions in
                // another function to make them static
                return () => param;
            }
            if (typeof param === 'object') {
                const wrapped = {};
                Object.keys(param).forEach(key => {
                    wrapped[key] = _wrapStatic(wrapped[key]);
                });
                return wrapped;
            }
            return param;
        }

        const _options = options || {};
        const { persistent, _static } = _options;

        let paramRepo;
        if (persistent) {
            paramRepo = 'persistentParams';
        } else {
            paramRepo = 'testParams';
        }
        let _paramValue;
        if (_static) {
            _paramValue = _wrapStatic(paramValue);
        } else {
            _paramValue = paramValue;
        }
        if (!paramPath) {
            this[paramRepo] = _paramValue;
            return undefined;
        }
        let param = this[paramRepo];
        let parent;
        paramPath.split('.').forEach(child => {
            parent = param;
            if (param[child] === undefined) {
                param[child] = {};
            }
            param = param[child];
        });
        parent[paramPath.split('.').slice(-1)[0]] = _paramValue;
        return undefined;
    }

    resetParam(paramPath, options) {
        this.setParam(paramPath, undefined, options);
    }

    resetTest() {
        this.testParams = {};
    }
}

module.exports = TestConfigurator;
