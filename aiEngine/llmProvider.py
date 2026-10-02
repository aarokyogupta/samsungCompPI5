import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
import httpx
import json
import os
import platform
import psutil
from pydantic import BaseModel, ConfigDict, Field, ValidationError
import re
import shutil
import sys
import threading
import time
from typing import Any, Callable, Optional, Sequence, TypeVar
import yaml

# Allow "python aiEngine/llmProvider.py" as well as "python -m aiEngine.llmProvider" from the project root
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Load API keys from .env (never from config.yaml) so secrets stay out of source control
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Load settings from YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

PROVIDER_CONFIG: dict = config.get("aiEngine", {}).get("llmProvider", {}) or {}

# "auto" tries local first (cloud first when throttled or complex), "local"/"cloud" pin one route, "none" = fallback only
ROUTING_MODE: str = str(PROVIDER_CONFIG.get("routingMode", "auto")).strip().lower()

# Hardware guardrails
MIN_FREE_RAM_GB: float = float(PROVIDER_CONFIG.get("minFreeRamGb", 2.5))
THERMAL_ZONE_PATH: str = str(PROVIDER_CONFIG.get("thermalZonePath", "/sys/class/thermal/thermal_zone0/temp"))
MAX_CPU_TEMP_C: float = float(PROVIDER_CONFIG.get("maxCpuTempC", 80.0))
NETWORK_PROBE_HOST: str = str(PROVIDER_CONFIG.get("networkProbeHost", "8.8.8.8"))
NETWORK_DNS_HOST: str = str(PROVIDER_CONFIG.get("networkDnsHost", "dns.google"))
NETWORK_TIMEOUT_SEC: float = float(PROVIDER_CONFIG.get("networkTimeoutSec", 2.0))
NETWORK_CACHE_SEC: float = float(PROVIDER_CONFIG.get("networkCacheSec", 30.0))

# Local edge LLM settings
LOCAL_BACKEND: str = str(PROVIDER_CONFIG.get("localBackend", "ollama")).strip().lower()
OLLAMA_MODEL: str = str(PROVIDER_CONFIG.get("ollamaModel", "llama3.2:3b"))
OLLAMA_BASE_URL: str = str(PROVIDER_CONFIG.get("ollamaBaseUrl", "http://localhost:11434")).rstrip("/")
OLLAMA_KEEP_ALIVE: str = str(PROVIDER_CONFIG.get("ollamaKeepAlive", "10m"))
GGUF_MODEL_PATH: str = os.path.expanduser(str(PROVIDER_CONFIG.get("ggufModelPath", "~/icmis/models/model.gguf")))
GRAMMAR_FILE: str = os.path.expanduser(str(PROVIDER_CONFIG.get("grammarFile", "") or ""))
LOCAL_THREADS: int = int(PROVIDER_CONFIG.get("localThreads", 3))
LOCAL_CONTEXT_TOKENS: int = int(PROVIDER_CONFIG.get("localContextTokens", 4096))
LOCAL_MAX_TOKENS: int = int(PROVIDER_CONFIG.get("localMaxTokens", 1536))
LOCAL_TIMEOUT_SEC: float = float(PROVIDER_CONFIG.get("localTimeoutSec", 45.0))
TEMPERATURE: float = float(PROVIDER_CONFIG.get("temperature", 0.0))

# Cloud fallback settings
CLOUD_PROVIDER: str = str(PROVIDER_CONFIG.get("cloudProvider", "openai")).strip().lower()
OPENAI_MODEL: str = str(PROVIDER_CONFIG.get("openaiModel", "gpt-4o-mini"))
ANTHROPIC_MODEL: str = str(PROVIDER_CONFIG.get("anthropicModel", "claude-haiku-4-5"))
CLOUD_MAX_TOKENS: int = int(PROVIDER_CONFIG.get("cloudMaxTokens", 2048))
CLOUD_TIMEOUT_SEC: float = float(PROVIDER_CONFIG.get("cloudTimeoutSec", 30.0))
CLOUD_MAX_ATTEMPTS: int = int(PROVIDER_CONFIG.get("cloudMaxAttempts", 3))
CLOUD_RETRY_DELAYS_SEC: tuple[float, ...] = tuple(float(delay) for delay in PROVIDER_CONFIG.get("cloudRetryDelaysSec", [1.0, 2.0, 5.0]))
CIRCUIT_COOLDOWN_SEC: float = float(PROVIDER_CONFIG.get("circuitCooldownSec", 300.0))
SELF_CORRECTION: bool = bool(PROVIDER_CONFIG.get("selfCorrection", True))

if ROUTING_MODE not in ("auto", "local", "cloud", "none"):
    raise ValueError("llmProvider routingMode must be 'auto', 'local', 'cloud' or 'none'.")
if LOCAL_BACKEND not in ("ollama", "llamacpp", "none") or CLOUD_PROVIDER not in ("openai", "anthropic", "none"):
    raise ValueError("llmProvider localBackend must be ollama/llamacpp/none and cloudProvider openai/anthropic/none.")
if MIN_FREE_RAM_GB < 0.0 or MAX_CPU_TEMP_C <= 0.0 or NETWORK_TIMEOUT_SEC <= 0.0 or NETWORK_CACHE_SEC < 0.0:
    raise ValueError("llmProvider RAM, temperature and network settings must be positive.")
if LOCAL_THREADS < 1 or LOCAL_THREADS >= (os.cpu_count() or 4):
    # Leave at least one core free for the camera stream and vision inference
    LOCAL_THREADS = max(1, min(LOCAL_THREADS, (os.cpu_count() or 4) - 1))
if LOCAL_CONTEXT_TOKENS <= LOCAL_MAX_TOKENS or LOCAL_MAX_TOKENS < 1 or CLOUD_MAX_TOKENS < 1:
    raise ValueError("llmProvider localContextTokens must exceed localMaxTokens, and token limits must be at least 1.")
if LOCAL_TIMEOUT_SEC <= 0.0 or CLOUD_TIMEOUT_SEC <= 0.0 or CLOUD_MAX_ATTEMPTS < 1 or not CLOUD_RETRY_DELAYS_SEC:
    raise ValueError("llmProvider timeouts must be positive, cloudMaxAttempts at least 1 and cloudRetryDelaysSec non-empty.")

ROUTE_LOCAL: str = "local"
ROUTE_CLOUD: str = "cloud"
ROUTE_FALLBACK: str = "terminalFallback"
FALLBACK_GENERATOR: str = "terminalFallback"
# Rough characters-per-token ratio used to decide whether a prompt fits the local context window
CHARS_PER_TOKEN: float = 3.5
# SDK errors that retrying cannot fix (bad keys, malformed payloads)
NON_RETRYABLE_ERRORS: frozenset[str] = frozenset({
    "AuthenticationError", "PermissionDeniedError", "BadRequestError", "NotFoundError", "UnprocessableEntityError",
})
AUTH_ERRORS: frozenset[str] = frozenset({"AuthenticationError", "PermissionDeniedError"})

# Markdown fences and preamble text that quantized models often wrap around JSON
CODE_FENCE_PATTERN = re.compile(r"```(?:json|JSON)?\s*(.*?)\s*```", re.DOTALL)

# Static reply for when every route fails, so the interface always has safe text to show
TERMINAL_FALLBACK: dict[str, Any] = {
    "summary": "Automated narrative unavailable: local and cloud language models could not produce a valid report.",
    "status": "LLM_UNAVAILABLE",
    "guidance": "Refer to the calculated risk metrics, which are unaffected by this failure.",
}

SELF_CORRECTION_SYSTEM_PROMPT: str = (
    "You repair malformed JSON. Return only a single valid JSON object that matches the supplied JSON schema. "
    "Keep every value from the broken JSON that is valid; do not invent new facts or numbers."
)

ModelT = TypeVar("ModelT", bound=BaseModel)


class ProviderError(RuntimeError):
    pass


# Request, hardware and result schemas

class LLMRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    system_prompt: str
    user_prompt: str
    # Base64-encoded images with their MIME type, e.g. {"data": "...", "media_type": "image/jpeg"}
    images: tuple[dict[str, str], ...] = ()
    # Forces the cloud route first when the caller knows the query is beyond the edge model
    complex_query: bool = False

    @classmethod
    def fromMessages(cls, messages: Sequence[Any], **kwargs: Any) -> "LLMRequest":
        # Accepts LangChain SystemMessage/HumanMessage objects or {"role", "content"} dictionaries
        systemParts: list[str] = []
        userParts: list[str] = []
        for message in messages:
            role = message.get("role") if isinstance(message, dict) else getattr(message, "type", "human")
            content = message.get("content") if isinstance(message, dict) else getattr(message, "content", "")
            (systemParts if role == "system" else userParts).append(str(content))
        return cls(system_prompt="\n\n".join(systemParts), user_prompt="\n\n".join(userParts), **kwargs)

    def estimatedTokens(self) -> int:
        return int((len(self.system_prompt) + len(self.user_prompt)) / CHARS_PER_TOKEN)


class HardwareStatus(BaseModel):
    free_ram_gb: float
    cpu_temperature_c: Optional[float] = None
    memory_sufficient: bool
    thermal_throttled: bool
    network_available: bool
    local_backend_ready: bool
    local_model_resident: bool = False
    local_viable: bool
    reasons: list[str] = Field(default_factory=list)
    assessed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LLMResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    output: Any
    route: str
    generated_by: str
    self_corrected: bool = False
    warnings: list[str] = Field(default_factory=list)
    hardware: Optional[HardwareStatus] = None

    @property
    def usedFallback(self) -> bool:
        return self.route == ROUTE_FALLBACK


# Environment initialization & hardware assessment

def getFreeRamGb() -> float:
    return psutil.virtual_memory().available / (1024 ** 3)


def readCpuTemperature(thermalZonePath: str = THERMAL_ZONE_PATH) -> Optional[float]:
    # The Pi exposes millidegrees Celsius; other platforms fall back to psutil or report None
    try:
        with open(thermalZonePath, "r") as thermalFile:
            return int(thermalFile.read().strip()) / 1000.0
    except (OSError, ValueError):
        pass
    sensorReader = getattr(psutil, "sensors_temperatures", None)
    if sensorReader is None:
        return None
    try:
        readings = [entry.current for entries in sensorReader().values() for entry in entries if entry.current]
    except (OSError, RuntimeError):
        return None
    return max(readings) if readings else None


async def pingHost(host: str = NETWORK_PROBE_HOST, timeoutSec: float = NETWORK_TIMEOUT_SEC) -> bool:
    # Uses the system ping binary so ICMP works without root raw sockets
    pingPath = shutil.which("ping")
    if pingPath is None:
        return False
    if platform.system() == "Windows":
        arguments = ["-n", "1", "-w", str(int(timeoutSec * 1000)), host]
    else:
        arguments = ["-c", "1", "-W", str(max(1, round(timeoutSec))), host]
    process: Optional[asyncio.subprocess.Process] = None
    try:
        process = await asyncio.create_subprocess_exec(
            pingPath, *arguments, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        return await asyncio.wait_for(process.wait(), timeout=timeoutSec + 1.0) == 0
    except (OSError, asyncio.TimeoutError):
        if process is not None and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        return False


async def resolveDns(host: str = NETWORK_DNS_HOST, timeoutSec: float = NETWORK_TIMEOUT_SEC) -> bool:
    try:
        await asyncio.wait_for(asyncio.get_running_loop().getaddrinfo(host, 443), timeout=timeoutSec)
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def checkNetwork() -> bool:
    # ICMP ping first; a DNS lookup covers networks (e.g. satellite uplinks) that drop ICMP
    return await pingHost() or await resolveDns()


# Primary route: local edge LLM (Ollama / llama.cpp)

class OllamaBackend:
    kind: str = ROUTE_LOCAL

    def __init__(self, model: str = OLLAMA_MODEL, baseUrl: str = OLLAMA_BASE_URL) -> None:
        self.model = model
        self.baseUrl = baseUrl
        self.name = f"ollama:{model}"

    async def probe(self) -> tuple[bool, bool, str]:
        # Returns (ready, resident, reason); a resident model already holds its RAM so the free-RAM check is skipped
        try:
            async with httpx.AsyncClient(timeout=NETWORK_TIMEOUT_SEC) as client:
                tags = (await client.get(f"{self.baseUrl}/api/tags")).json()
                running = (await client.get(f"{self.baseUrl}/api/ps")).json()
        except (httpx.HTTPError, ValueError) as error:
            return False, False, f"Ollama server unreachable at {self.baseUrl} ({type(error).__name__})"
        modelNames = {self.normalizeName(entry.get("name", "")) for entry in tags.get("models", [])}
        if self.normalizeName(self.model) not in modelNames:
            return False, False, f"Ollama model {self.model!r} not pulled (run: ollama pull {self.model})"
        resident = self.normalizeName(self.model) in {self.normalizeName(entry.get("name", "")) for entry in running.get("models", [])}
        return True, resident, ""

    @staticmethod
    def normalizeName(name: str) -> str:
        return name if ":" in name else f"{name}:latest"

    async def setKeepAlive(self, keepAlive: Any) -> None:
        # An empty /api/generate request only loads (or with keep_alive 0, unloads) the model weights
        async with httpx.AsyncClient(timeout=httpx.Timeout(LOCAL_TIMEOUT_SEC * 4, connect=NETWORK_TIMEOUT_SEC)) as client:
            response = await client.post(f"{self.baseUrl}/api/generate", json={"model": self.model, "keep_alive": keepAlive})
            response.raise_for_status()

    async def warmUp(self) -> None:
        await self.setKeepAlive(OLLAMA_KEEP_ALIVE)

    async def release(self) -> None:
        try:
            await self.setKeepAlive(0)
        except httpx.HTTPError:
            pass

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        userMessage: dict[str, Any] = {"role": "user", "content": request.user_prompt}
        if request.images:
            userMessage["images"] = [image["data"] for image in request.images]
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": request.system_prompt}, userMessage],
            "stream": False,
            # Ollama compiles the JSON schema into a llama.cpp grammar, so only schema-shaped JSON can be sampled
            "format": outputSchema,
            "keep_alive": OLLAMA_KEEP_ALIVE,
            "options": {
                "num_thread": LOCAL_THREADS,
                "num_ctx": LOCAL_CONTEXT_TOKENS,
                "num_predict": maxTokens,
                "temperature": TEMPERATURE,
            },
        }
        # Closing the connection on timeout makes Ollama abort the generation server-side
        async with httpx.AsyncClient(timeout=httpx.Timeout(LOCAL_TIMEOUT_SEC + 5.0, connect=NETWORK_TIMEOUT_SEC)) as client:
            response = await client.post(f"{self.baseUrl}/api/chat", json=payload)
            response.raise_for_status()
            return str(response.json().get("message", {}).get("content", ""))


class LlamaCppBackend:
    kind: str = ROUTE_LOCAL

    def __init__(self, modelPath: str = GGUF_MODEL_PATH, grammarFile: str = GRAMMAR_FILE) -> None:
        self.modelPath = modelPath
        self.grammarFile = grammarFile
        self.name = f"llamacpp:{os.path.basename(modelPath)}"
        self.model: Any = None
        self.grammarCache: dict[str, Any] = {}
        self.loadLock = asyncio.Lock()
        # Serialises generations, including a timed-out one that is still winding down in its thread
        self.generationLock = threading.Lock()

    async def probe(self) -> tuple[bool, bool, str]:
        if not os.path.isfile(self.modelPath):
            return False, False, f"GGUF model not found at {self.modelPath}"
        try:
            import llama_cpp  # noqa: F401
        except ImportError:
            return False, False, "llama-cpp-python is not installed"
        return True, self.model is not None, ""

    async def ensureLoaded(self) -> None:
        async with self.loadLock:
            if self.model is not None:
                return
            from llama_cpp import Llama

            # mmap keeps the 4/5-bit weights paged from disk instead of copying them, avoiding SD-card swap
            self.model = await asyncio.to_thread(
                Llama,
                model_path=self.modelPath,
                n_threads=LOCAL_THREADS,
                n_threads_batch=LOCAL_THREADS,
                n_ctx=LOCAL_CONTEXT_TOKENS,
                use_mmap=True,
                verbose=False,
            )

    async def warmUp(self) -> None:
        await self.ensureLoaded()

    def releaseBlocking(self) -> None:
        # Waits for any in-flight generation so the weights are never freed underneath it
        with self.generationLock:
            if self.model is not None:
                self.model.close()
                self.model = None
            self.grammarCache.clear()

    async def release(self) -> None:
        async with self.loadLock:
            await asyncio.to_thread(self.releaseBlocking)

    def getGrammar(self, outputSchema: dict[str, Any]) -> Any:
        from llama_cpp import LlamaGrammar

        if self.grammarFile:
            cacheKey = f"file:{self.grammarFile}"
            if cacheKey not in self.grammarCache:
                self.grammarCache[cacheKey] = LlamaGrammar.from_file(self.grammarFile, verbose=False)
            return self.grammarCache[cacheKey]
        cacheKey = json.dumps(outputSchema, sort_keys=True)
        if cacheKey not in self.grammarCache:
            try:
                # GBNF grammar generated from the Pydantic schema restricts sampling to valid, schema-shaped JSON
                self.grammarCache[cacheKey] = LlamaGrammar.from_json_schema(cacheKey, verbose=False)
            except Exception:
                from llama_cpp.llama_grammar import JSON_GBNF

                # Schemas the converter cannot handle still get the generic JSON grammar
                self.grammarCache[cacheKey] = LlamaGrammar.from_string(JSON_GBNF, verbose=False)
        return self.grammarCache[cacheKey]

    def generateBlocking(self, request: LLMRequest, grammar: Any, maxTokens: int, cancelEvent: threading.Event) -> str:
        with self.generationLock:
            parts: list[str] = []
            stream = self.model.create_chat_completion(
                messages=[
                    {"role": "system", "content": request.system_prompt},
                    {"role": "user", "content": request.user_prompt},
                ],
                grammar=grammar,
                max_tokens=maxTokens,
                temperature=TEMPERATURE,
                stream=True,
            )
            # Streaming lets the timeout guardrail stop the thread between tokens
            for chunk in stream:
                if cancelEvent.is_set():
                    stream.close()
                    raise ProviderError("llama.cpp generation interrupted by the timeout guardrail")
                parts.append(chunk["choices"][0].get("delta", {}).get("content") or "")
            return "".join(parts)

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        if request.images:
            raise ProviderError("llama.cpp backend is text-only; image requests need the cloud route")
        await self.ensureLoaded()
        grammar = await asyncio.to_thread(self.getGrammar, outputSchema)
        cancelEvent = threading.Event()
        try:
            return await asyncio.to_thread(self.generateBlocking, request, grammar, maxTokens, cancelEvent)
        except asyncio.CancelledError:
            # Threads cannot be killed, so the flag makes the token loop exit at its next step
            cancelEvent.set()
            raise


# Circuit breaker shared by every backend that can be used as the cloud route

class CircuitBreaker:
    name: str = "backend"
    consecutiveFailures: int = 0
    circuitOpenUntil: float = 0.0

    def circuitReason(self) -> str:
        remainingSec = self.circuitOpenUntil - time.monotonic()
        return f"{self.name} circuit breaker open for {remainingSec:.0f}s" if remainingSec > 0 else ""

    def recordSuccess(self) -> None:
        self.consecutiveFailures = 0
        self.circuitOpenUntil = 0.0

    def openCircuit(self) -> None:
        self.consecutiveFailures += 1
        self.circuitOpenUntil = time.monotonic() + CIRCUIT_COOLDOWN_SEC


# Secondary route: cloud LLM fallback (OpenAI / Anthropic)

class CloudBackend(CircuitBreaker):
    kind: str = ROUTE_CLOUD
    apiKeyVariable: str = ""

    def __init__(self, model: str) -> None:
        self.model = model
        self.client: Any = None

    @property
    def apiKey(self) -> Optional[str]:
        return os.getenv(self.apiKeyVariable) or None

    async def probe(self) -> tuple[bool, bool, str]:
        if not self.apiKey:
            return False, False, f"{self.apiKeyVariable} is not set in .env"
        if reason := self.circuitReason():
            return False, False, reason
        return True, False, ""

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        raise NotImplementedError

    async def release(self) -> None:
        if self.client is not None:
            await self.client.close()
            self.client = None


class OpenAIBackend(CloudBackend):
    apiKeyVariable = "OPENAI_API_KEY"

    def __init__(self, model: str = OPENAI_MODEL) -> None:
        super().__init__(model)
        self.name = f"openai:{model}"

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        if self.client is None:
            from openai import AsyncOpenAI

            # SDK retries are disabled so the circuit breaker owns the backoff schedule
            self.client = AsyncOpenAI(api_key=self.apiKey, timeout=CLOUD_TIMEOUT_SEC, max_retries=0)
        userContent: Any = request.user_prompt
        if request.images:
            userContent = [{"type": "text", "text": request.user_prompt}] + [
                {"type": "image_url", "image_url": {"url": f"data:{image.get('media_type', 'image/jpeg')};base64,{image['data']}"}}
                for image in request.images
            ]
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": request.system_prompt},
                {"role": "user", "content": userContent},
            ],
            # JSON mode guarantees a syntactically valid object; Pydantic then checks the schema
            response_format={"type": "json_object"},
            max_tokens=maxTokens,
            temperature=TEMPERATURE,
        )
        return str(response.choices[0].message.content or "")


class AnthropicBackend(CloudBackend):
    apiKeyVariable = "ANTHROPIC_API_KEY"
    toolName: str = "submit_structured_output"

    def __init__(self, model: str = ANTHROPIC_MODEL) -> None:
        super().__init__(model)
        self.name = f"anthropic:{model}"

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        if self.client is None:
            from anthropic import AsyncAnthropic

            self.client = AsyncAnthropic(api_key=self.apiKey, timeout=CLOUD_TIMEOUT_SEC, max_retries=0)
        userContent: list[dict[str, Any]] = [
            {"type": "image", "source": {"type": "base64", "media_type": image.get("media_type", "image/jpeg"), "data": image["data"]}}
            for image in request.images
        ]
        userContent.append({"type": "text", "text": request.user_prompt})
        # A forced tool call is Anthropic's structured output: the tool input must follow the JSON schema
        response = await self.client.messages.create(
            model=self.model,
            system=request.system_prompt,
            messages=[{"role": "user", "content": userContent}],
            tools=[{"name": self.toolName, "description": "Return the structured result.", "input_schema": outputSchema}],
            tool_choice={"type": "tool", "name": self.toolName},
            max_tokens=maxTokens,
            temperature=TEMPERATURE,
        )
        for block in response.content:
            if getattr(block, "type", None) == "tool_use":
                return json.dumps(block.input)
        return "".join(getattr(block, "text", "") for block in response.content)


class LangChainBackend(CircuitBreaker):
    # Adapts any LangChain chat model (ChatOllama, FakeListChatModel in tests) to the provider interface

    def __init__(self, llm: Any, kind: str = ROUTE_LOCAL) -> None:
        self.llm = llm
        self.kind = kind
        self.name = f"langchain:{getattr(llm, 'model', type(llm).__name__)}"

    async def probe(self) -> tuple[bool, bool, str]:
        reason = self.circuitReason()
        return not reason, True, reason

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        from langchain_core.messages import HumanMessage, SystemMessage

        response = await self.llm.ainvoke([SystemMessage(content=request.system_prompt), HumanMessage(content=request.user_prompt)])
        return str(getattr(response, "content", response))


def createLocalBackend(backendName: str = LOCAL_BACKEND) -> Optional[Any]:
    if backendName == "ollama":
        return OllamaBackend()
    if backendName == "llamacpp":
        return LlamaCppBackend()
    return None


def createCloudBackend(providerName: str = CLOUD_PROVIDER) -> Optional[CloudBackend]:
    if providerName == "openai":
        return OpenAIBackend()
    if providerName == "anthropic":
        return AnthropicBackend()
    return None


# Output parsing & error handling

def cleanModelOutput(rawText: str) -> str:
    text = rawText.strip()
    fenced = CODE_FENCE_PATTERN.search(text)
    if fenced:
        text = fenced.group(1).strip()
    # Drop any preamble or trailing commentary around the outermost JSON object
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        text = text[start:end + 1]
    return text


def parseStructuredOutput(
    rawText: str, outputModel: type[ModelT], validator: Optional[Callable[[ModelT], ModelT]] = None
) -> ModelT:
    parsed = outputModel.model_validate_json(cleanModelOutput(rawText))
    # Optional semantic checks (e.g. figures must match the payload) raise ValueError to reject the output
    return validator(parsed) if validator is not None else parsed


def summarizeError(error: BaseException, limit: int = 600) -> str:
    message = f"{type(error).__name__}: {error}".replace("\n", " ")
    return message if len(message) <= limit else message[:limit] + "..."


def buildCorrectionRequest(rawText: str, error: BaseException, outputSchema: dict[str, Any]) -> LLMRequest:
    return LLMRequest(
        system_prompt=SELF_CORRECTION_SYSTEM_PROMPT,
        user_prompt=(
            "Fix this JSON to match this schema.\n\n"
            f"JSON schema:\n{json.dumps(outputSchema)}\n\n"
            f"Validation errors:\n{summarizeError(error, 2000)}\n\n"
            f"Broken JSON:\n{rawText[:12000]}"
        ),
    )


# Routing & execution

class LLMProvider:
    def __init__(
        self,
        localBackend: Any = "default",
        cloudBackend: Any = "default",
        routingMode: str = ROUTING_MODE,
        assessHardware: bool = True,
    ) -> None:
        self.localBackend = createLocalBackend() if localBackend == "default" else localBackend
        self.cloudBackend = createCloudBackend() if cloudBackend == "default" else cloudBackend
        self.routingMode = routingMode
        self.assessHardware = assessHardware
        self.localLock = asyncio.Lock()
        self.networkCache: Optional[tuple[float, bool]] = None

    async def isNetworkAvailable(self) -> bool:
        now = time.monotonic()
        if self.networkCache is not None and now - self.networkCache[0] < NETWORK_CACHE_SEC:
            return self.networkCache[1]
        available = await checkNetwork()
        self.networkCache = (now, available)
        return available

    def markNetworkDown(self) -> None:
        self.networkCache = (time.monotonic(), False)

    async def warmUp(self, timeoutSec: float = LOCAL_TIMEOUT_SEC * 4) -> str:
        # Loads the local weights before the first request so it does not pay the load latency
        if self.localBackend is None or not hasattr(self.localBackend, "warmUp"):
            return "skipped: no local backend"
        status = await self.assessEnvironment()
        if not status.local_viable:
            return "skipped: " + ("; ".join(status.reasons) or "local route not viable")
        try:
            async with self.localLock:
                await asyncio.wait_for(self.localBackend.warmUp(), timeout=timeoutSec)
        except asyncio.TimeoutError:
            return f"failed: warm-up exceeded {timeoutSec:.0f}s"
        except Exception as error:
            return f"failed: {summarizeError(error, 200)}"
        return f"ready: {self.localBackend.name}"

    async def release(self) -> None:
        # Frees local model RAM and closes cloud HTTP clients on shutdown
        for backend in (self.localBackend, self.cloudBackend):
            if backend is not None and hasattr(backend, "release"):
                try:
                    await backend.release()
                except Exception:
                    pass

    async def assessEnvironment(self) -> HardwareStatus:
        reasons: list[str] = []
        if not self.assessHardware:
            return HardwareStatus(
                free_ram_gb=getFreeRamGb(), memory_sufficient=True, thermal_throttled=False, network_available=True,
                local_backend_ready=self.localBackend is not None, local_model_resident=True,
                local_viable=self.localBackend is not None, reasons=["hardware assessment disabled"],
            )
        freeRamGb = getFreeRamGb()
        temperature = readCpuTemperature()
        thermalThrottled = temperature is not None and temperature >= MAX_CPU_TEMP_C
        if thermalThrottled:
            reasons.append(f"CPU at {temperature:.1f}C >= {MAX_CPU_TEMP_C:.0f}C: local inference throttled")
        # Network probe and local backend probe run concurrently to keep assessment under a few seconds
        needsNetwork = self.cloudBackend is not None
        networkTask = self.isNetworkAvailable() if needsNetwork else asyncio.sleep(0, result=False)
        probeTask = self.localBackend.probe() if self.localBackend is not None else asyncio.sleep(0, result=(False, False, "no local backend configured"))
        networkAvailable, (localReady, resident, probeReason) = await asyncio.gather(networkTask, probeTask)
        if probeReason:
            reasons.append(probeReason)
        memorySufficient = resident or freeRamGb >= MIN_FREE_RAM_GB
        if localReady and not memorySufficient:
            reasons.append(f"only {freeRamGb:.2f}GB free RAM (< {MIN_FREE_RAM_GB:.1f}GB needed to load the model)")
        if needsNetwork and not networkAvailable:
            reasons.append("no internet connectivity for the cloud route")
        return HardwareStatus(
            free_ram_gb=round(freeRamGb, 2),
            cpu_temperature_c=temperature,
            memory_sufficient=memorySufficient,
            thermal_throttled=thermalThrottled,
            network_available=networkAvailable,
            local_backend_ready=localReady,
            local_model_resident=resident,
            local_viable=localReady and memorySufficient and not thermalThrottled,
            reasons=reasons,
        )

    def isComplexRequest(self, request: LLMRequest) -> bool:
        if request.complex_query or (request.images and not isinstance(self.localBackend, OllamaBackend)):
            return True
        # Prompts that would overflow the edge model's context window go to the cloud first
        return request.estimatedTokens() + LOCAL_MAX_TOKENS > LOCAL_CONTEXT_TOKENS

    async def planRoutes(self, request: LLMRequest, status: HardwareStatus, warnings: list[str]) -> list[str]:
        cloudReady = False
        if self.cloudBackend is not None:
            cloudReady, _, cloudReason = await self.cloudBackend.probe()
            if cloudReason:
                warnings.append(f"llmProvider: cloud route unavailable: {cloudReason}")
        cloudViable = cloudReady and status.network_available
        localViable = status.local_viable
        if self.routingMode == "none":
            return []
        if self.routingMode == "local":
            return [ROUTE_LOCAL] if localViable else []
        if self.routingMode == "cloud":
            return [ROUTE_CLOUD] if cloudViable else []
        if cloudViable and (status.thermal_throttled or self.isComplexRequest(request)):
            return [ROUTE_CLOUD] + ([ROUTE_LOCAL] if localViable else [])
        return ([ROUTE_LOCAL] if localViable else []) + ([ROUTE_CLOUD] if cloudViable else [])

    async def runLocal(self, request: LLMRequest, outputSchema: dict[str, Any]) -> str:
        # One generation at a time on the Pi; the timer covers queueing, loading and generation
        async def guardedGeneration() -> str:
            async with self.localLock:
                return await self.localBackend.generate(request, outputSchema, LOCAL_MAX_TOKENS)

        try:
            return await asyncio.wait_for(guardedGeneration(), timeout=LOCAL_TIMEOUT_SEC)
        except asyncio.TimeoutError as error:
            raise ProviderError(f"local generation exceeded the {LOCAL_TIMEOUT_SEC:.0f}s guardrail") from error

    async def runCloud(self, request: LLMRequest, outputSchema: dict[str, Any], warnings: list[str]) -> str:
        backend: CloudBackend = self.cloudBackend
        lastError: Optional[BaseException] = None
        for attempt in range(CLOUD_MAX_ATTEMPTS):
            try:
                rawText = await asyncio.wait_for(
                    backend.generate(request, outputSchema, CLOUD_MAX_TOKENS), timeout=CLOUD_TIMEOUT_SEC + 5.0
                )
                backend.recordSuccess()
                return rawText
            except Exception as error:
                lastError = error
                errorName = type(error).__name__
                warnings.append(f"llmProvider: {backend.name} attempt {attempt + 1}/{CLOUD_MAX_ATTEMPTS} failed: {summarizeError(error, 200)}")
                if errorName in AUTH_ERRORS:
                    backend.openCircuit()
                    break
                if errorName in NON_RETRYABLE_ERRORS:
                    break
                if attempt + 1 < CLOUD_MAX_ATTEMPTS:
                    # Exponential backoff for rate limits and patchy satellite links (1s, 2s, then 5s)
                    await asyncio.sleep(CLOUD_RETRY_DELAYS_SEC[min(attempt, len(CLOUD_RETRY_DELAYS_SEC) - 1)])
        else:
            # Every attempt failed on a transient error: trip the breaker and recheck the link next time
            backend.openCircuit()
            if lastError is not None and type(lastError).__name__ in ("APIConnectionError", "APITimeoutError", "TimeoutError"):
                self.markNetworkDown()
        raise ProviderError(f"{backend.name} failed after retries") from lastError

    async def selfCorrect(
        self, rawText: str, error: BaseException, outputModel: type[ModelT], outputSchema: dict[str, Any],
        validator: Optional[Callable[[ModelT], ModelT]], warnings: list[str],
    ) -> Optional[ModelT]:
        try:
            correctedText = await self.runCloud(buildCorrectionRequest(rawText, error, outputSchema), outputSchema, warnings)
            return parseStructuredOutput(correctedText, outputModel, validator)
        except Exception as correctionError:
            warnings.append(f"llmProvider: self-correction failed: {summarizeError(correctionError, 300)}")
            return None

    async def generateStructured(
        self,
        request: LLMRequest,
        outputModel: type[ModelT],
        validator: Optional[Callable[[ModelT], ModelT]] = None,
        fallback: Optional[Callable[[], Any]] = None,
    ) -> LLMResult:
        warnings: list[str] = []
        status = await self.assessEnvironment()
        warnings.extend(f"llmProvider: {reason}" for reason in status.reasons if self.assessHardware)
        outputSchema = outputModel.model_json_schema()
        routes = await self.planRoutes(request, status, warnings)
        selfCorrectionUsed = False

        for route in routes:
            backend = self.localBackend if route == ROUTE_LOCAL else self.cloudBackend
            try:
                rawText = await (self.runLocal(request, outputSchema) if route == ROUTE_LOCAL else self.runCloud(request, outputSchema, warnings))
            except Exception as error:
                warnings.append(f"llmProvider: {route} route ({backend.name}) failed: {summarizeError(error, 300)}")
                continue
            try:
                output = parseStructuredOutput(rawText, outputModel, validator)
                return LLMResult(output=output, route=route, generated_by=backend.name, warnings=warnings, hardware=status)
            except (ValidationError, ValueError) as error:
                warnings.append(f"llmProvider: {backend.name} output rejected: {summarizeError(error)}")
                # One self-correction pass through the cloud when the link is up and the breaker is closed
                cloudReady = self.cloudBackend is not None and status.network_available and (await self.cloudBackend.probe())[0]
                if SELF_CORRECTION and not selfCorrectionUsed and cloudReady:
                    selfCorrectionUsed = True
                    corrected = await self.selfCorrect(rawText, error, outputModel, outputSchema, validator, warnings)
                    if corrected is not None:
                        return LLMResult(
                            output=corrected, route=route, generated_by=f"{backend.name}+{self.cloudBackend.name}",
                            self_corrected=True, warnings=warnings, hardware=status,
                        )

        if not routes:
            warnings.append("llmProvider: no viable LLM route (local and cloud unavailable)")
        # Terminal fallback: the caller's deterministic output, or the static safe dictionary
        output = fallback() if fallback is not None else dict(TERMINAL_FALLBACK)
        return LLMResult(output=output, route=ROUTE_FALLBACK, generated_by=FALLBACK_GENERATOR, warnings=warnings, hardware=status)


# Demo backend and entry point

class ScriptedBackend(CircuitBreaker):
    # Returns canned replies in order; used by the demo and tests to exercise routing without hardware

    def __init__(self, replies: Sequence[str | BaseException], kind: str = ROUTE_LOCAL, name: str = "scripted") -> None:
        self.replies = list(replies)
        self.kind = kind
        self.name = name
        self.calls = 0

    async def probe(self) -> tuple[bool, bool, str]:
        reason = self.circuitReason()
        return not reason, True, reason

    async def generate(self, request: LLMRequest, outputSchema: dict[str, Any], maxTokens: int) -> str:
        reply = self.replies[min(self.calls, len(self.replies) - 1)]
        self.calls += 1
        if isinstance(reply, BaseException):
            raise reply
        return reply


class DemoAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    risk_level: int = Field(ge=1, le=5)


async def main() -> None:
    provider = LLMProvider()
    status = await provider.assessEnvironment()
    print(f"Free RAM: {status.free_ram_gb:.2f}GB (sufficient: {status.memory_sufficient})")
    print(f"CPU temperature: {status.cpu_temperature_c if status.cpu_temperature_c is not None else 'unavailable'} "
          f"(throttled: {status.thermal_throttled})")
    print(f"Network available: {status.network_available}; local viable: {status.local_viable}")
    for reason in status.reasons:
        print(f"  - {reason}")

    request = LLMRequest(
        system_prompt="Return JSON with keys summary (string) and risk_level (integer 1-5).",
        user_prompt="CRI is 82.4 and the threat subscore dominates. Summarise in one sentence.",
    )
    liveResult = await provider.generateStructured(request, DemoAssessment)
    print(f"\nLive run -> route {liveResult.route} via {liveResult.generated_by}: {liveResult.output}")

    # Scripted run: the local model wraps its JSON in markdown and breaks the schema, so the cloud repairs it
    scriptedProvider = LLMProvider(
        localBackend=ScriptedBackend(['```json\n{"summary": "Threat pressure drives CRI 82.4.", "risk_level": 9}\n```']),
        cloudBackend=ScriptedBackend(['{"summary": "Threat pressure drives CRI 82.4.", "risk_level": 5}'], kind=ROUTE_CLOUD, name="scriptedCloud"),
        assessHardware=False,
    )
    scriptedResult = await scriptedProvider.generateStructured(request, DemoAssessment)
    print(f"Scripted run -> route {scriptedResult.route} via {scriptedResult.generated_by} "
          f"(self-corrected: {scriptedResult.self_corrected}): {scriptedResult.output}")
    for warning in scriptedResult.warnings:
        print(f"  - {warning}")


if __name__ == "__main__":
    asyncio.run(main())
