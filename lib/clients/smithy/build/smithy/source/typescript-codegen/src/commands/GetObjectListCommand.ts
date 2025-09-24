// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetObjectListInput,
  GetObjectListOutput,
} from "../models/models_0";
import {
  de_GetObjectListCommand,
  se_GetObjectListCommand,
} from "../protocols/Aws_restJson1";
import { getEndpointPlugin } from "@smithy/middleware-endpoint";
import { getSerdePlugin } from "@smithy/middleware-serde";
import { Command as $Command } from "@smithy/smithy-client";
import { MetadataBearer as __MetadataBearer } from "@smithy/types";

/**
 * @public
 */
export type { __MetadataBearer };
export { $Command };
/**
 * @public
 *
 * The input for {@link GetObjectListCommand}.
 */
export interface GetObjectListCommandInput extends GetObjectListInput {}
/**
 * @public
 *
 * The output of {@link GetObjectListCommand}.
 */
export interface GetObjectListCommandOutput extends GetObjectListOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetObjectListCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetObjectListCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetObjectListInput
 *   Bucket: "STRING_VALUE", // required
 * };
 * const command = new GetObjectListCommand(input);
 * const response = await client.send(command);
 * // { // GetObjectListOutput
 * //   Contents: [ // ObjectMDList
 * //     { // ObjectMD
 * //       key: "STRING_VALUE",
 * //       value: "STRING_VALUE",
 * //     },
 * //   ],
 * //   CommonPrefixes: [ // CommonPrefixList
 * //     "STRING_VALUE",
 * //   ],
 * //   IsTruncated: true || false,
 * //   Delimiter: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param GetObjectListCommandInput - {@link GetObjectListCommandInput}
 * @returns {@link GetObjectListCommandOutput}
 * @see {@link GetObjectListCommandInput} for command's `input` shape.
 * @see {@link GetObjectListCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class GetObjectListCommand extends $Command.classBuilder<GetObjectListCommandInput, GetObjectListCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetObjectList", {

  })
  .n("BackbeatClient", "GetObjectListCommand")
  .f(void 0, void 0)
  .ser(se_GetObjectListCommand)
  .de(de_GetObjectListCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetObjectListInput;
      output: GetObjectListOutput;
  };
  sdk: {
      input: GetObjectListCommandInput;
      output: GetObjectListCommandOutput;
  };
};
}
