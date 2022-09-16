gcloud functions deploy ocr-extract \
--entry-point functions.OcrProcessImage \
--runtime java17 \
--memory 512MB \
--trigger-bucket YOUR_IMAGE_BUCKET_NAME \
--set-env-vars "^:^GCP_PROJECT=YOUR_GCP_PROJECT_ID:TRANSLATE_TOPIC=YOUR_TRANSLATE_TOPIC_NAME:RESULT_TOPIC=YOUR_RESULT_TOPIC_NAME:TO_LANG=es,en,fr,ja"
exit