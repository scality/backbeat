const assert = require('assert');
const http = require('http');

const werelogs = require('werelogs');
const PromAPIClient = require('../../../lib/clients/PromAPIClient');

const log = new werelogs.Logger('test:PromAPIClient');

describe('PromAPIClient', () => {
    let promServerMock;
    let promClient;
    let expectedUrl;
    let httpStatus;
    const mockMetricsResult = [{
        metric: {
            petType: 'puppy',
        },
        value: [
            1557338726.55,
            '100',
        ],
    }];
    const mockSuccessResponse = {
        status: 'success',
        data: {
            resultType: 'vector',
            result: mockMetricsResult,
        },
    };
    const mockErrorResponse = 'Server Error';
    const invalidQuery = 'max(awesomeness';
    const mockInvalidQueryResponse = {
        status: 'error',
        errorType: 'bad_data',
        error: 'parse error at char 16: unclosed left parenthesis',
    };

    before(() => {
        promServerMock = http.createServer((req, res) => {
            const buffers = [];
            req.on('data', data => {
                buffers.push(data);
            });
            req.on('end', () => {
                assert.strictEqual(req.url, expectedUrl);
                res.writeHead(httpStatus, {
                    'Content-Type': 'application/json',
                });
                let response;
                if (httpStatus !== 200) {
                    response = mockErrorResponse;
                } else if (req.url.endsWith(invalidQuery)) {
                    response = mockInvalidQueryResponse;
                } else {
                    response = mockSuccessResponse;
                }
                res.end(JSON.stringify(response));
            });
        });
        promServerMock.listen(7777);
        promClient = new PromAPIClient('localhost:7777');
    });
    after(() => {
        promServerMock.close();
    });

    beforeEach(() => {
        expectedUrl = undefined;
        httpStatus = 200;
    });

    function setExpectedUrl(_expectedUrl) {
        expectedUrl = _expectedUrl;
    }
    function setHTTPStatus(_httpStatus) {
        httpStatus = _httpStatus;
    }
    it('should send a valid query and parse answer', done => {
        setExpectedUrl(
            '/api/v1/query?query=awesomeness%7BpetType%3D%22puppy%22%7D');
        promClient.executeQuery({
            query: 'awesomeness{petType="puppy"}',
        }, log, (err, results) => {
            assert.ifError(err);
            assert.deepStrictEqual(results, mockMetricsResult);
            done();
        });
    });

    it('should detect error on HTTP status 500', done => {
        setExpectedUrl('/api/v1/query?query=max(awesomeness)');
        setHTTPStatus(500);
        promClient.executeQuery({
            query: 'max(awesomeness)',
        }, log, err => {
            assert(err);
            assert.strictEqual(err.code, 500);
            assert.strictEqual(err.message, 'InternalError');
            done();
        });
    });

    it('should detect error in JSON response', done => {
        setExpectedUrl(`/api/v1/query?query=${invalidQuery}`);
        promClient.executeQuery({
            query: invalidQuery,
        }, log, err => {
            assert(err);
            assert.strictEqual(err.code, 500);
            assert.strictEqual(err.message, 'InternalError');
            assert.strictEqual(
                err.description,
                'parse error at char 16: unclosed left parenthesis');
            done();
        });
    });

    it('should build a basic metric selector', () => {
        const selector = PromAPIClient.buildMetricSelector('awesomeness');
        assert.strictEqual(selector, 'awesomeness{}');
    });

    it('should build a metric selector with labels', () => {
        const selector = PromAPIClient.buildMetricSelector('awesomeness', {
            petType: 'puppy',
        });
        assert.strictEqual(selector, 'awesomeness{petType="puppy"}');
    });

    it('should build a metric selector with regexp', () => {
        const selector =
              PromAPIClient.buildMetricSelector('awesomeness', null, {
                  petType: 'pup.*',
              });
        assert.strictEqual(selector, 'awesomeness{petType=~"pup.*"}');
    });

    it('should build a metric selector with labels and regexps', () => {
        const selector = PromAPIClient.buildMetricSelector(
            'awesomeness', {
                eyeColor: 'brown',
            }, {
                petType: 'puppy|kitty',
            });
        assert.strictEqual(
            selector, 'awesomeness{eyeColor="brown",petType=~"puppy|kitty"}');
    });
});
