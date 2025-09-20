import boto3



def lambda_handler(event, context):
    import os, time, json, urllib.parse, urllib.request, boto3, os.path as op

    transcribe = boto3.client("transcribe")
    translate  = boto3.client("translate")
    polly      = boto3.client("polly")
    s3         = boto3.client("s3")

    src_lang   = os.getenv("SOURCE_LANG", "en-US")
    tgt_lang   = os.getenv("TARGET_LANG", "es")
    voice_id   = os.getenv("POLLY_VOICE", "Lupe")
    out_prefix = (os.getenv("OUTPUT_PREFIX", "audio-outputs/").rstrip("/") + "/")

    for r in event["Records"]:
        b = r["s3"]["bucket"]["name"]
        k = urllib.parse.unquote_plus(r["s3"]["object"]["key"])
        base = op.splitext(op.basename(k))[0]
        fmt  = op.splitext(k)[1].lstrip(".").lower() or "mp3"
        job  = f"tx-{base}-{context.aws_request_id[:8]}"

        # 1) Transcribe
        transcribe.start_transcription_job(
            TranscriptionJobName=job,
            LanguageCode=src_lang,
            MediaFormat=fmt,
            Media={"MediaFileUri": f"s3://{b}/{k}"},
        )
        while True:
            j = transcribe.get_transcription_job(TranscriptionJobName=job)["TranscriptionJob"]
            s = j["TranscriptionJobStatus"]
            if s in ("COMPLETED", "FAILED"): break
            time.sleep(3)
        if s == "FAILED":
            continue

        # 2) Read transcript text
        uri  = j["Transcript"]["TranscriptFileUri"]
        text = json.loads(urllib.request.urlopen(uri).read().decode("utf-8"))["results"]["transcripts"][0]["transcript"]

        # 3) Translate → Polly
        ttxt  = translate.translate_text(Text=text, SourceLanguageCode="en", TargetLanguageCode=tgt_lang)["TranslatedText"]
        audio = polly.synthesize_speech(Text=ttxt, OutputFormat="mp3", VoiceId=voice_id)["AudioStream"].read()

        # 4) Save MP3 beside your data (same bucket)
        s3.put_object(Bucket=b, Key=f"{out_prefix}{base}-{tgt_lang}.mp3", Body=audio, ContentType="audio/mpeg")

    return {"ok": True}
