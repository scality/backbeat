// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  MultipleBackendPutObjectTaggingInput,
  MultipleBackendPutObjectTaggingOutput,
} from "../models/models_0";
import {
  de_MultipleBackendPutObjectTaggingCommand,
  se_MultipleBackendPutObjectTaggingCommand,
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
export type MultipleBackendPutObjectTaggingCommandInputType = Omit<MultipleBackendPutObjectTaggingInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link MultipleBackendPutObjectTaggingCommand}.
 */
export interface MultipleBackendPutObjectTaggingCommandInput extends MultipleBackendPutObjectTaggingCommandInputType {}
/**
 * @public
 *
 * The output of {@link MultipleBackendPutObjectTaggingCommand}.
 */
export interface MultipleBackendPutObjectTaggingCommandOutput extends MultipleBackendPutObjectTaggingOutput, __MetadataBearer {}

/**
 * Adds or updates tags for an object in multiple backend storage
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, MultipleBackendPutObjectTaggingCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, MultipleBackendPutObjectTaggingCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // MultipleBackendPutObjectTaggingInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   StorageType: "STRING_VALUE",
 *   StorageClass: "STRING_VALUE", // required
 *   DataStoreVersionId: "STRING_VALUE",
 *   Tags: "STRING_VALUE",
 *   SourceBucket: "STRING_VALUE",
 *   SourceVersionId: "STRING_VALUE",
 *   ReplicationEndpointSite: "STRING_VALUE",
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new MultipleBackendPutObjectTaggingCommand(input);
 * const response = await client.send(command);
 * // { // MultipleBackendPutObjectTaggingOutput
 * //   versionId: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param MultipleBackendPutObjectTaggingCommandInput - {@link MultipleBackendPutObjectTaggingCommandInput}
 * @returns {@link MultipleBackendPutObjectTaggingCommandOutput}
 * @see {@link MultipleBackendPutObjectTaggingCommandInput} for command's `input` shape.
 * @see {@link MultipleBackendPutObjectTaggingCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 * @public
 */
export class MultipleBackendPutObjectTaggingCommand extends $Command.classBuilder<MultipleBackendPutObjectTaggingCommandInput, MultipleBackendPutObjectTaggingCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "MultipleBackendPutObjectTagging", {

  })
  .n("BackbeatClient", "MultipleBackendPutObjectTaggingCommand")
  .f(void 0, void 0)
  .ser(se_MultipleBackendPutObjectTaggingCommand)
  .de(de_MultipleBackendPutObjectTaggingCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: MultipleBackendPutObjectTaggingInput;
      output: MultipleBackendPutObjectTaggingOutput;
  };
  sdk: {
      input: MultipleBackendPutObjectTaggingCommandInput;
      output: MultipleBackendPutObjectTaggingCommandOutput;
  };
};
}
