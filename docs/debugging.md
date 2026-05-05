Had to use 5 different virtual env, one for dashboard, one for databricks, one for postgres, one for producers(kafka), and one for airflow.

decided to use gemini(free llm) and grock as backup (also free).

The original architecture diagram I drew said "Claude / GPT call" in the reasoning node. Let me update your mental model: that node now just says "LLM call (Gemini)". Functionally identical. LangGraph doesn't care which provider — that's the whole point of LangChain's abstraction layer.
If you want to be fancy and impress recruiters, the README architecture section can describe it as "LLM-agnostic reasoning layer with provider failover (Gemini primary, Groq fallback)" — that's literally a 20-line code addition (try Gemini, on rate limit error, retry with Groq) and it's a real production pattern.

started using databricks community edition for the following reasons:
Continuous streaming costs money. No real company runs continuous streaming for prototypes. They use micro-batch on a schedule — exactly what Free Edition forces.
Your Airflow layer was already going to schedule this. Originally I had Airflow only doing the model retrain and agent batch reports. We just expand its responsibility to also trigger the Spark micro-batches every 5–10 minutes. This is the standard production pattern — Airflow + scheduled Spark micro-batches is more common than continuous streaming in industry.
No expiration, no card. Free Edition is permanent, no 14-day clock to manage. Cancel that calendar reminder I had you set.

wrote a healthcheck script to check the entire connectivity everyday

linkedin ready - PulseTrade is a real-time financial intelligence platform that ingests live market and news streams through Kafka and processes them on Databricks using a Delta Lake medallion architecture. When the system flags an unusual price movement, an agentic AI layer built with LangGraph and MCP automatically investigates by querying recent news, sentiment scores, and live web search to produce a human-readable market brief — all orchestrated by Airflow and deployed on Kubernetes with full observability.

yahoo finance problem and solution 
My Yahoo Finance producer was throwing JSON parse errors that initially looked like rate-limiting. After investigating, I found Yahoo had started detecting yfinance's HTTP signature and returning HTML challenge pages. I fixed it by routing requests through curl_cffi to impersonate Chrome's TLS fingerprint, which is a known community fix. I also added exponential backoff retries since transient failures still happen.

even this dint workout so had to switch to finnhub, 
"I started with yfinance because it's the most popular Python library, but I hit reliability issues — Yahoo aggressively blocks bots and yfinance scrapes their endpoints. Even with browser impersonation via curl_cffi, internal yfinance bugs around metadata parsing made it unreliable. I switched to Finnhub's official REST API which has documented contracts, predictable rate limits, and richer data including high/low/open/close in a single call. The producer became more reliable and the data more useful for downstream analytics."

using unity catalog instead of dbfs
"I use Unity Catalog Volumes for streaming checkpoints rather than DBFS. This is the modern Databricks pattern — UC Volumes give you proper governance, ACLs, and audit logs over file storage, treating files as first-class assets like tables. It's also forward-compatible with Free Edition's stricter security model."

A nuance worth understanding (for interview prep)
You might be asked: "Why is your producer polling every 30 seconds if the data only changes during market hours?"
Good answer:

"The producer always runs because the infrastructure needs to handle 24/7 ingestion. Markets being closed is a data-source concern, not a pipeline concern. Downstream consumers (Spark, the agent) shouldn't need to know about market schedules — they process whatever lands in Kafka. If I wanted to be cost-conscious, I'd skip Yahoo polls outside market hours, but the Kafka pipeline itself stays on."

When asked in interviews about data quality you can say:

"I treat bronze as an append-only audit log of raw events including malformed ones, then enforce data quality contracts at the silver layer. Bad rows get filtered with explicit predicates rather than crashing the pipeline."

When asked "tell me about a hard debugging session," this is gold:

"FinBERT inference inside Spark was failing with a read-only filesystem error. The traceback was 200 lines deep — the actual cause was buried at the bottom: Hugging Face was trying to write model weights to /home, which is read-only on Databricks Free Edition's serverless compute. The fix was to point HF_HOME at a Unity Catalog Volume — writable storage that's also persistent across worker restarts, so the 440MB model only downloads once. I had to set the env vars inside the UDF function itself because env vars don't propagate from driver to executors in Spark."

"Integrating Hugging Face FinBERT inference inside Spark on Databricks Free Edition required navigating three layered constraints: Free Edition's home filesystem is read-only, Unity Catalog Volumes don't support Hugging Face's new Xet protocol, and Spark workers don't propagate driver env vars. The clean solution was pre-downloading the model on the driver and pointing the worker UDF at a local snapshot path — which is the standard production pattern anyway because it avoids per-worker network overhead."

"Spark Connect (the client used by Databricks Free Edition serverless) doesn't implement the legacy RDD API, so batch_df.rdd.isEmpty() fails. The fix was switching to batch_df.isEmpty() directly. This is a real consideration for production Databricks code now that Spark Connect is becoming the default execution mode — it forces you to stay on the higher-level DataFrame APIs and avoid RDD-era patterns."

"Free Edition's serverless compute disables several Spark APIs that production clusters allow: .rdd.*, .persist(), .cache(). These are all server-side state operations that don't fit serverless's stateless model. Adapting code for Spark Connect taught me to think more carefully about which Spark operations require persistent state and which are pure transformations."