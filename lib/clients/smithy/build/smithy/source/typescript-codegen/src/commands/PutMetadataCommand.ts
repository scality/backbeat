// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  PutMetadataInput,
  PutMetadataOutput,
} from "../models/models_0";
import {
  de_PutMetadataCommand,
  se_PutMetadataCommand,
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
export type PutMetadataCommandInputType = Omit<PutMetadataInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link PutMetadataCommand}.
 */
export interface PutMetadataCommandInput extends PutMetadataCommandInputType {}
/**
 * @public
 *
 * The output of {@link PutMetadataCommand}.
 */
export interface PutMetadataCommandOutput extends PutMetadataOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, PutMetadataCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, PutMetadataCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // PutMetadataInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   VersionId: "STRING_VALUE",
 *   AccountId: "STRING_VALUE",
 *   ContentMD5: "STRING_VALUE",
 *   ReplicationContent: "STRING_VALUE",
 *   VersioningRequired: true || false,
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new PutMetadataCommand(input);
 * const response = await client.send(command);
 * // { // PutMetadataOutput
 * //   versionId: "STRING_VALUE",
 * // };
 *
 * ```
 *
 * @param PutMetadataCommandInput - {@link PutMetadataCommandInput}
 * @returns {@link PutMetadataCommandOutput}
 * @see {@link PutMetadataCommandInput} for command's `input` shape.
 * @see {@link PutMetadataCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class PutMetadataCommand extends $Command.classBuilder<PutMetadataCommandInput, PutMetadataCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "PutMetadata", {

  })
  .n("BackbeatClient", "PutMetadataCommand")
  .f(void 0, void 0)
  .ser(se_PutMetadataCommand)
  .de(de_PutMetadataCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: PutMetadataInput;
      output: PutMetadataOutput;
  };
  sdk: {
      input: PutMetadataCommandInput;
      output: PutMetadataCommandOutput;
  };
};
}
