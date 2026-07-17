#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# Minaris Swarm — LLM Auto-Configuration Script (Protocol Startup)
# Reads the master .env file and exports all LLM credentials to the session.
# Run at workspace boot: source /opt/minaris/picoclaw/init_llm_config.sh
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

ENV_FILE="/root/minaris/.env"
BOLD="\033[1m"
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
NC="\033[0m"

echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}${BOLD}║   MINARIS SWARM — LLM Auto-Configuration v2.0      ║${NC}"
echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ── 1. Check .env exists ─────────────────────────────────────────────────────
if [ ! -f "$ENV_FILE" ]; then
  echo -e "${RED}✗ FATAL: $ENV_FILE not found. Cannot proceed.${NC}"
  return 1 2>/dev/null || exit 1
fi
echo -e "${GREEN}✓${NC} Found master .env at ${BOLD}$ENV_FILE${NC}"

# ── 2. Helper: extract value from .env ────────────────────────────────────────
extract_env() {
  local key="$1"
  local val
  val=$(grep -E "^${key}=" "$ENV_FILE" | tail -1 | cut -d'=' -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/#.*//' | sed 's/[[:space:]]*$//')
  echo "$val"
}

# ── 3. Extract & Export LLM Credentials ───────────────────────────────────────

# --- Z.ai / Zhipu (Priority 1) ---
export ZHIPU_API_KEY=$(extract_env "ZHIPU_API_KEY")
export ZHIPU_BASE_URL=$(extract_env "ZHIPU_BASE_URL")
export ZAI_BASE_URL=$(extract_env "ZAI_BASE_URL")

# --- Qwen / Alibaba (Priority 2) ---
export QWEN_API_KEY=$(extract_env "QWEN_API_KEY")
export QWEN_API_BASE_URL=$(extract_env "QWEN_API_BASE_URL")

# --- MiniMax (Priority 3) ---
export MINIMAX_API_KEY=$(extract_env "MINIMAX_API_KEY")

# --- Supporting Providers ---
export OPENROUTER_API_KEY=$(extract_env "OPENROUTER_API_KEY")
export GOOGLE_AI_STUDIO_API_KEY=$(extract_env "GOOGLE_AI_STUDIO_API_KEY")
export DEEPSEEK_API_KEY=$(extract_env "DEEPSEEK_API_KEY")
export POLLINATIONS_API_KEY=$(extract_env "POLLINATIONS_API_KEY")
export TAVILY_API_KEY=$(extract_env "TAVILY_API_KEY")

# --- N8N Bridge ---
export N8N_WEBHOOK_URL=$(extract_env "WEBHOOK_URL")
export N8N_API_KEY=$(extract_env "ATOM_N8N_API_KEY")

# ── 4. Validation & Status Table ──────────────────────────────────────────────
echo ""
echo -e "${BOLD}┌──────────────────────────────────────────────────────────────┐${NC}"
echo -e "${BOLD}│  LLM Provider Credential Status                             │${NC}"
echo -e "${BOLD}├─────────────┬──────────────────────┬────────────────────────┤${NC}"
printf  "${BOLD}│ %-11s │ %-20s │ %-22s │${NC}\n" "Priority" "Provider" "Status"
echo -e "${BOLD}├─────────────┼──────────────────────┼────────────────────────┤${NC}"

check_key() {
  local priority="$1"
  local name="$2"
  local key_val="$3"
  local url_val="${4:-}"
  if [ -n "$key_val" ] && [ "$key_val" != "" ]; then
    local masked="${key_val:0:8}...${key_val: -4}"
    printf "│ ${GREEN}%-11s${NC} │ %-20s │ ${GREEN}✓ %-20s${NC} │\n" "$priority" "$name" "$masked"
  else
    printf "│ ${RED}%-11s${NC} │ %-20s │ ${RED}✗ MISSING              ${NC} │\n" "$priority" "$name"
  fi
}

check_key "P1 Default"  "Z.ai (GLM)"     "$ZHIPU_API_KEY"    "$ZAI_BASE_URL"
check_key "P2 Fallback" "Qwen (Alibaba)" "$QWEN_API_KEY"     "$QWEN_API_BASE_URL"
check_key "P3 Fallback" "MiniMax"         "$MINIMAX_API_KEY"
check_key "Support"     "OpenRouter"      "$OPENROUTER_API_KEY"
check_key "Support"     "Google AI"       "$GOOGLE_AI_STUDIO_API_KEY"
check_key "Support"     "DeepSeek"        "$DEEPSEEK_API_KEY"

echo -e "${BOLD}└─────────────┴──────────────────────┴────────────────────────┘${NC}"

# ── 5. Model Routing Summary ─────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Model Routing Hierarchy:${NC}"
echo -e "  ${GREEN}►${NC} P1 Text:   ${BOLD}GLM 4.7 Flash${NC} → fallback → ${BOLD}GLM 4.5 Flash${NC}"
echo -e "  ${GREEN}►${NC} P1 Vision: ${BOLD}GLM 4.6v Flash${NC} (mandatory for screenshots)"
echo -e "  ${YELLOW}►${NC} P2 Text:   ${BOLD}qwen3.5-flash${NC} / ${BOLD}qwen3-coder-flash${NC}"
echo -e "  ${YELLOW}►${NC} P2 Vision: ${BOLD}qwen3-vl-flash${NC} (fallback if GLM 4.6v down)"
echo -e "  ${RED}►${NC} P3 Text:   ${BOLD}MiniMax M2.5${NC} (last resort)"
echo ""

# ── 6. Export Swarm-specific env vars ─────────────────────────────────────────
export SWARM_LLM_DEFAULT_PROVIDER="zai"
export SWARM_LLM_DEFAULT_MODEL="glm-4.7-flash"
export SWARM_LLM_VISION_MODEL="glm-4.6v-flash"
export SWARM_LLM_FALLBACK_ORDER="zai,qwen,minimax"
export SWARM_PIPELINE_FIRECRAWL_URL="http://firecrawl:3002"
export SWARM_QDRANT_URL="http://minaris_memory:6333"

echo -e "${GREEN}${BOLD}✓ Autoconfiguração Concluída. Modelos mapeados. Iniciando Pipeline Multimodal.${NC}"
echo ""
