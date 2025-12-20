package temporal_joins;


/**
 * Eine Factory-Klasse zur einfachen Erzeugung von {@link TemporalJoinStrategy}-Instanzen.
 * <p>
 * Dies entkoppelt den Client-Code von den konkreten Implementierungsklassen der Strategien.
 */
public final class JoinStrategyFactory {

    // Privater Konstruktor, da dies eine Utility-Klasse ist.
    private JoinStrategyFactory() {}

    /**
     * Gibt eine Instanz der passenden Join-Strategie basierend auf einem Namen zurück.
     *
     * @param strategyName Der Name der Strategie (nicht case-sensitive).
     *                     Gültige Werte sind z.B. "linear", "interpolate", "forward_fill", "locf".
     * @return Eine Instanz von {@link TemporalJoinStrategy}.
     * @throws IllegalArgumentException wenn der Strategiename unbekannt ist.
     */
    public static TemporalJoinStrategy getStrategy(String strategyName) {
        if (strategyName == null || strategyName.trim().isEmpty()) {
            throw new IllegalArgumentException("Strategiename darf nicht null oder leer sein.");
        }

        switch (strategyName.toLowerCase().trim()) {
            case "linear":
            case "interpolate":
            case "interpolate_linear":
                return new LinearInterpolationJoinStrategy();

            case "forward_fill":
            case "locf": // Last Observation Carried Forward
                return new ForwardFillJoinStrategy();


            case "resample":
            case "resample_avg":
            case "aggregate":
                return new ResamplingJoinStrategy();

            // Hier könnten zukünftige Strategien hinzugefügt werden
            // case "resample_avg":
            //    return new ResamplingAverageJoinStrategy();

            default:
                throw new IllegalArgumentException("Unbekannte Temporal Join Strategie: " + strategyName);
        }

    }
}