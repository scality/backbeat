// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  PutBucketIndexesInput,
  PutBucketIndexesOutput,
} from "../models/models_0";
import {
  de_PutBucketIndexesCommand,
  se_PutBucketIndexesCommand,
} from "../protocols/Aws_restJson1";
import { getEndpointPlugin } from "@smithy/middleware-endpoint";
import { getSerdePlugin } from "@smithy/middleware-serde";
import { Command as $Command } from "@smithy/smithy-client";
import {
  BlobPayloadInputTypes,
  MetadataBearer as __MetadataBearer,
} from "@smithy/types";

/**
 * @public
 */
export type { __MetadataBearer };
export { $Command };
/**
 * @public
 */
export type PutBucketIndexesCommandInputType = Omit<PutBucketIndexesInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link PutBucketIndexesCommand}.
 */
export interface PutBucketIndexesCommandInput extends PutBucketIndexesCommandInputType {}
/**
 * @public
 *
 * The output of {@link PutBucketIndexesCommand}.
 */
export interface PutBucketIndexesCommandOutput extends PutBucketIndexesOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, PutBucketIndexesCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, PutBucketIndexesCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // PutBucketIndexesInput
 *   Bucket: "STRING_VALUE", // required
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new PutBucketIndexesCommand(input);
 * const response = await client.send(command);
 * // {};
 *
 * ```
 *
 * @param PutBucketIndexesCommandInput - {@link PutBucketIndexesCommandInput}
 * @returns {@link PutBucketIndexesCommandOutput}
 * @see {@link PutBucketIndexesCommandInput} for command's `input` shape.
 * @see {@link PutBucketIndexesCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class PutBucketIndexesCommand extends $Command.classBuilder<PutBucketIndexesCommandInput, PutBucketIndexesCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "PutBucketIndexes", {

  })
  .n("BackbeatClient", "PutBucketIndexesCommand")
  .f(void 0, void 0)
  .ser(se_PutBucketIndexesCommand)
  .de(de_PutBucketIndexesCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: PutBucketIndexesInput;
      output: {};
  };
  sdk: {
      input: PutBucketIndexesCommandInput;
      output: PutBucketIndexesCommandOutput;
  };
};
}
