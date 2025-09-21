def lambda_handler(event, context):
    import os, time, json, urllib.parse, urllib.request, boto3, os.path as op, logging

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.info("Event: %s", json.dumps(event)[:2000])

    transcribe = boto3.client("transcribe")
    translate  = boto3.client("translate")
    polly      = boto3.client("polly")
    s3         = boto3.client("s3")

    src_lang_full = os.getenv("SOURCE_LANG", "en-US")    
    src_lang_tx   = src_lang_full.split("-")[0] or "en"  
    tgt_lang      = os.getenv("TARGET_LANG", "es")
    voice_id      = os.getenv("POLLY_VOICE", "Lupe")
    out_prefix    = (os.getenv("OUTPUT_PREFIX", "audio-outputs/").rstrip("/") + "/")

    def put_obj(bucket: str, key: str, body: bytes, content_type: str):
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        logger.info("Wrote s3://%s/%s (%s bytes, %s)", bucket, key, len(body), content_type)

    records = event.get("Records") or []
    if not records:
        logger.warning("No S3 Records in event; nothing to do.")
        return {"ok": True, "processed": 0}

    processed = 0
    for r in records:
        try:
            b = r["s3"]["bucket"]["name"]
            k = urllib.parse.unquote_plus(r["s3"]["object"]["key"])
            base = op.splitext(op.basename(k))[0]
            fmt  = op.splitext(k)[1].lstrip(".").lower() or "mp3"
            job  = f"tx-{base}-{context.aws_request_id[:8]}"

            logger.info("Start Transcribe job=%s for s3://%s/%s fmt=%s", job, b, k, fmt)
            uri_in = f"s3://{b}/{k}"

            try:
                logger.info("Lambda region=%s", boto3.session.Session().region_name)
                loc = s3.get_bucket_location(Bucket=b)["LocationConstraint"] or "us-east-1"
                logger.info("Bucket region=%s", loc)
            except Exception as e:
                logger.warning("Could not get bucket location: %s", e)

            s3.head_object(Bucket=b, Key=k)

            transcribe.start_transcription_job(
                TranscriptionJobName=job,
                LanguageCode=src_lang_full,
                MediaFormat=fmt,
                Media={"MediaFileUri": uri_in},
            )
            while True:
                tj = transcribe.get_transcription_job(TranscriptionJobName=job)["TranscriptionJob"]
                status = tj["TranscriptionJobStatus"]
                if status in ("COMPLETED", "FAILED"):
                    logger.info("Transcribe job=%s status=%s", job, status)
                    break
                time.sleep(3)
            if status == "FAILED":
                logger.error("Transcribe failed for %s: %s", k, tj.get("FailureReason"))
                continue

            transcript_uri = tj["Transcript"]["TranscriptFileUri"]
            logger.debug("Transcript URI: %s", transcript_uri)
            try:
                transcript_body_bytes = urllib.request.urlopen(transcript_uri, timeout=10).read()
                transcript_json = json.loads(transcript_body_bytes.decode("utf-8"))
            except Exception:
                logger.exception("Fetching transcript failed (check VPC/NAT or bucket perms).")
                continue

            text = transcript_json["results"]["transcripts"][0]["transcript"]
            logger.info("Transcript length=%d chars", len(text))

            transcript_json_key = f"{out_prefix}{base}-transcript.json"
            transcript_txt_key  = f"{out_prefix}{base}-{src_lang_full}-transcript.txt"
            put_obj(b, transcript_json_key, json.dumps(transcript_json).encode("utf-8"), "application/json")
            put_obj(b, transcript_txt_key,  (text + "\n").encode("utf-8"),               "text/plain; charset=utf-8")

            ttxt = translate.translate_text(Text=text, SourceLanguageCode=src_lang_tx, TargetLanguageCode=tgt_lang)["TranslatedText"]
            logger.info("Translated to %s length=%d chars", tgt_lang, len(ttxt))

            translated_txt_key = f"{out_prefix}{base}-translated-{tgt_lang}.txt"
            put_obj(b, translated_txt_key, (ttxt + "\n").encode("utf-8"), "text/plain; charset=utf-8")

            audio_stream = polly.synthesize_speech(Text=ttxt, OutputFormat="mp3", VoiceId=voice_id)["AudioStream"].read()
            mp3_key = f"{out_prefix}{base}-{tgt_lang}.mp3"
            put_obj(b, mp3_key, audio_stream, "audio/mpeg")

            processed += 1

        except Exception:
            logger.exception("Unhandled error processing record")

    return {"ok": True, "processed": processed}
