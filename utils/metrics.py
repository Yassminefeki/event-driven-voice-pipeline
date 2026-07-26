import logging
try:
    import jiwer
    HAS_JIWER = True
except ImportError:
    HAS_JIWER = False

logger = logging.getLogger(__name__)


def calculate_metrics(reference: str, hypothesis: str) -> tuple[float, float]:
    """Calculates Word Error Rate (WER) and Character Error Rate (CER)."""
    if not reference and not hypothesis:
        return 0.0, 0.0

    if not reference:
        return 1.0, 1.0

    if HAS_JIWER:
        try:
            wer = jiwer.wer(reference, hypothesis)
            cer = jiwer.cer(reference, hypothesis)
            return round(float(wer), 4), round(float(cer), 4)
        except Exception as e:
            logger.error(f"⚠️ Error in Jiwer calculation: {e}")
            return -1.0, -1.0
    else:
        # Fallback simple metric when jiwer is unavailable
        ref_words = reference.split()
        hyp_words = hypothesis.split()
        wer = 0.0 if ref_words == hyp_words else 1.0

        ref_chars = list(reference)
        hyp_chars = list(hypothesis)
        cer = 0.0 if ref_chars == hyp_chars else 1.0

        return round(wer, 4), round(cer, 4)