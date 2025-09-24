// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  GetBucketCseqInput,
  GetBucketCseqOutput,
} from "../models/models_0";
import {
  de_GetBucketCseqCommand,
  se_GetBucketCseqCommand,
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
 * The input for {@link GetBucketCseqCommand}.
 */
export interface GetBucketCseqCommandInput extends GetBucketCseqInput {}
/**
 * @public
 *
 * The output of {@link GetBucketCseqCommand}.
 */
export interface GetBucketCseqCommandOutput extends GetBucketCseqOutput, __MetadataBearer {}

/**
 * Retrieves bucket sequence information
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, GetBucketCseqCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, GetBucketCseqCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // GetBucketCseqInput
 *   Bucket: "STRING_VALUE", // required
 * };
 * const command = new GetBucketCseqCommand(input);
 * const response = await client.send(command);
 * // { // GetBucketCseqOutput
 * //   CseqInfo: [ // CseqInfoList
 * //     { // CseqInfo
 * //       cseq: Number("int"),
 * //     },
 * //   ],
 * // };
 *
 * ```
 *
 * @param GetBucketCseqCommandInput - {@link GetBucketCseqCommandInput}
 * @returns {@link GetBucketCseqCommandOutput}
 * @see {@link GetBucketCseqCommandInput} for command's `input` shape.
 * @see {@link GetBucketCseqCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class GetBucketCseqCommand extends $Command.classBuilder<GetBucketCseqCommandInput, GetBucketCseqCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "GetBucketCseq", {

  })
  .n("BackbeatClient", "GetBucketCseqCommand")
  .f(void 0, void 0)
  .ser(se_GetBucketCseqCommand)
  .de(de_GetBucketCseqCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: GetBucketCseqInput;
      output: GetBucketCseqOutput;
  };
  sdk: {
      input: GetBucketCseqCommandInput;
      output: GetBucketCseqCommandOutput;
  };
};
}
