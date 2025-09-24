// smithy-typescript generated code
import {
  BackbeatClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BackbeatClient";
import { commonParams } from "../endpoint/EndpointParameters";
import {
  PutDataInput,
  PutDataOutput,
} from "../models/models_0";
import {
  de_PutDataCommand,
  se_PutDataCommand,
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
export type PutDataCommandInputType = Omit<PutDataInput, "Body"> & {
  Body?: BlobPayloadInputTypes;
};

/**
 * @public
 *
 * The input for {@link PutDataCommand}.
 */
export interface PutDataCommandInput extends PutDataCommandInputType {}
/**
 * @public
 *
 * The output of {@link PutDataCommand}.
 */
export interface PutDataCommandOutput extends PutDataOutput, __MetadataBearer {}

/**
 * @public
 *
 * @example
 * Use a bare-bones client and the command you need to make an API call.
 * ```javascript
 * import { BackbeatClient, PutDataCommand } from "@backbeat-service/client"; // ES Modules import
 * // const { BackbeatClient, PutDataCommand } = require("@backbeat-service/client"); // CommonJS import
 * const client = new BackbeatClient(config);
 * const input = { // PutDataInput
 *   Bucket: "STRING_VALUE", // required
 *   Key: "STRING_VALUE", // required
 *   ContentMD5: "STRING_VALUE",
 *   CanonicalID: "STRING_VALUE",
 *   VersioningRequired: true || false,
 *   Body: new Uint8Array(), // e.g. Buffer.from("") or new TextEncoder().encode("")
 * };
 * const command = new PutDataCommand(input);
 * const response = await client.send(command);
 * // { // PutDataOutput
 * //   LocationsData: "DOCUMENT_VALUE",
 * // };
 *
 * ```
 *
 * @param PutDataCommandInput - {@link PutDataCommandInput}
 * @returns {@link PutDataCommandOutput}
 * @see {@link PutDataCommandInput} for command's `input` shape.
 * @see {@link PutDataCommandOutput} for command's `response` shape.
 * @see {@link BackbeatClientResolvedConfig | config} for BackbeatClient's `config` shape.
 *
 * @throws {@link BackbeatServiceException}
 * <p>Base exception class for all service exceptions from Backbeat service.</p>
 *
 *
 */
export class PutDataCommand extends $Command.classBuilder<PutDataCommandInput, PutDataCommandOutput, BackbeatClientResolvedConfig, ServiceInputTypes, ServiceOutputTypes>()
  .ep(commonParams)
      .m(function (this: any, Command: any, cs: any, config: BackbeatClientResolvedConfig, o: any) {
          return [

  getSerdePlugin(config, this.serialize, this.deserialize),
  getEndpointPlugin(config, Command.getEndpointParameterInstructions()),
      ];
  })
  .s("Backbeat", "PutData", {

  })
  .n("BackbeatClient", "PutDataCommand")
  .f(void 0, void 0)
  .ser(se_PutDataCommand)
  .de(de_PutDataCommand)
.build() {
/** @internal type navigation helper, not in runtime. */
declare protected static __types: {
  api: {
      input: PutDataInput;
      output: PutDataOutput;
  };
  sdk: {
      input: PutDataCommandInput;
      output: PutDataCommandOutput;
  };
};
}
