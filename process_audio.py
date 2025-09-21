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

    src_lang   = os.getenv("SOURCE_LANG", "en-US")
    tgt_lang   = os.getenv("TARGET_LANG", "es")
    voice_id   = os.getenv("POLLY_VOICE", "Lupe")
    out_prefix = (os.getenv("OUTPUT_PREFIX", "audio-outputs/").rstrip("/") + "/")

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

            uri = f"s3://{b}/{k}"
            
            logger.info("Transcribe input (bucket/key): %s %s  -> %s", b, k, uri)

            # Log the Lambda/Transcribe region and bucket region
            import boto3
            logger.info("Lambda region=%s", boto3.session.Session().region_name)
            try:
                loc = s3.get_bucket_location(Bucket=b)["LocationConstraint"] or "us-east-1"
                logger.info("Bucket region=%s", loc)
            except Exception as e:
                logger.warning("Could not get bucket location: %s", e)

            # Verify the object actually exists (and Lambda can see it)
            try:
                s3.head_object(Bucket=b, Key=k)
            except Exception as e:
                logger.error("Object missing or Lambda can't read it: %s (%s)", uri, e)
                return {"ok": False}

            transcribe.start_transcription_job(
                TranscriptionJobName=job,
                LanguageCode=src_lang,
                MediaFormat=fmt,
                Media={"MediaFileUri": f"s3://{b}/{k}"},
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

            uri = tj["Transcript"]["TranscriptFileUri"]
            logger.debug("Transcript URI: %s", uri)
            try:
                body = urllib.request.urlopen(uri, timeout=10).read().decode("utf-8")
            except Exception:
                logger.exception("Fetching transcript failed (check VPC/NAT or bucket perms).")
                continue
            text = json.loads(body)["results"]["transcripts"][0]["transcript"]
            logger.info("Transcript length=%d chars", len(text))

            ttxt  = translate.translate_text(Text=text, SourceLanguageCode="en", TargetLanguageCode=tgt_lang)["TranslatedText"]
            logger.info("Translated to %s length=%d chars", tgt_lang, len(ttxt))

            audio = polly.synthesize_speech(Text=ttxt, OutputFormat="mp3", VoiceId=voice_id)["AudioStream"].read()
            out_key = f"{out_prefix}{base}-{tgt_lang}.mp3"
            s3.put_object(Bucket=b, Key=out_key, Body=audio, ContentType="audio/mpeg")
            logger.info("Wrote MP3 to s3://%s/%s", b, out_key)

            processed += 1
        except Exception:
            logger.exception("Unhandled error processing record")
    return {"ok": True, "processed": processed}
