/*
 * This software includes the work that is distributed in the Apache License 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package functions;

// [START functions_ocr_process]

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.vision.v1.*;
import com.google.gson.Gson;
import functions.eventpojos.GcsEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

// [END functions_ocr_process]

// [START functions_ocr_setup]
public class OcrDenseTextDocumentDetection implements BackgroundFunction<GcsEvent> {
    // TODO<developer> set these environment variables
    private static final Logger logger = Logger.getLogger(OcrDenseTextDocumentDetection.class.getName());

    private static final String RESULT_BUCKET = System.getenv("RESULT_BUCKET");

    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();

    private static final Gson gson = new Gson();

    // [END functions_ocr_setup]

    // [START functions_ocr_process]
    @Override
    public void accept(GcsEvent gcsEvent, Context context) {

        // Validate parameters
        String bucket = gcsEvent.getBucket();
        if (bucket == null) {
            throw new IllegalArgumentException("Missing bucket parameter");
        }
        String filename = gcsEvent.getName();
        if (filename == null) {
            throw new IllegalArgumentException("Missing name parameter");
        }

        detectText(bucket, filename);
    }
    // [END functions_ocr_process]

    // [START functions_ocr_detect]
    private void detectText(String bucket, String filename) {
        logger.info("Looking for text in image " + filename);

        List<AnnotateImageRequest> visionRequests = new ArrayList<>();
        String gcsPath = String.format("gs://%s/%s", bucket, filename);
        logger.info(String.format("gcsPath %s", gcsPath));

        ImageSource imgSource = ImageSource.newBuilder().setGcsImageUri(gcsPath).build();
        Image img = Image.newBuilder().setSource(imgSource).build();

        Feature textFeature = Feature.newBuilder().setType(Feature.Type.DOCUMENT_TEXT_DETECTION).build();
        AnnotateImageRequest visionRequest =
                AnnotateImageRequest.newBuilder().addFeatures(textFeature).setImage(img).build();
        visionRequests.add(visionRequest);

        // Detect text in an image using the Cloud Vision API
        AnnotateImageResponse visionResponse;
        try (ImageAnnotatorClient client = ImageAnnotatorClient.create()) {
            visionResponse = client.batchAnnotateImages(visionRequests).getResponses(0);
            if (visionResponse == null || !visionResponse.hasFullTextAnnotation()) {
                logger.info(String.format("Image %s contains no text", filename));
                return;
            }

            if (visionResponse.hasError()) {
                // Log error
                logger.log(
                        Level.SEVERE, "Error in vision API call: " + visionResponse.getError().getMessage());
                return;
            }
        } catch (IOException e) {
            // Log error (since IOException cannot be thrown by a Cloud Function)
            logger.log(Level.SEVERE, "Error detecting text: " + e.getMessage(), e);
            return;
        }

        String text = visionResponse.getFullTextAnnotation().getText();
        logger.info("Extracted text from image: " + text);

        String newFileName = String.format("%s.json", filename);

        logger.info(String.format("Saving result to %s in bucket %s", newFileName, RESULT_BUCKET));
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(RESULT_BUCKET, newFileName)).build();
        STORAGE.create(blobInfo, gson.toJson(visionResponse).getBytes(StandardCharsets.UTF_8));
        logger.info("File saved");
    }
    // [END functions_ocr_detect]

    // [START functions_ocr_process]
    // [START functions_ocr_setup]
}
// [END functions_ocr_setup]
// [END functions_ocr_process]
