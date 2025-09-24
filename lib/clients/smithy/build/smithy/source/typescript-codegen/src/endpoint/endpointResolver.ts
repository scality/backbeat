// smithy-typescript generated code
import { EndpointParameters } from "./EndpointParameters";
import { ruleSet } from "./ruleset";
import { awsEndpointFunctions } from "@aws-sdk/util-endpoints";
import {
  EndpointV2,
  Logger,
} from "@smithy/types";
import {
  EndpointCache,
  EndpointParams,
  customEndpointFunctions,
  resolveEndpoint,
} from "@smithy/util-endpoints";

const cache = new EndpointCache({
    size: 50,
    params: ["Endpoint",
 "Region",
 "UseDualStack",
 "UseFIPS"]
});

export const defaultEndpointResolver = (
    endpointParams: EndpointParameters,
    context: { logger?: Logger } = {}
): EndpointV2 => {
    return cache.get(endpointParams as EndpointParams, () => resolveEndpoint(ruleSet, {
        endpointParams: endpointParams as EndpointParams,
        logger: context.logger,
    }));
};

customEndpointFunctions.aws = awsEndpointFunctions;
