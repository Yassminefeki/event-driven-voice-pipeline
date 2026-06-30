import jiwer

def calculate_metrics(reference: str, hypothesis: str) -> tuple[float, float]:
    """Calcule le WER (Word Error Rate) et le CER (Character Error Rate)."""
    if not reference:
        return 1.0, 1.0
    
    try:
        wer = jiwer.wer(reference, hypothesis)
        cer = jiwer.cer(reference, hypothesis)
        return float(wer), float(cer)
    except Exception as e:
        print(f"⚠️ Erreur lors du calcul Jiwer : {e}")
        return -1.0, -1.0