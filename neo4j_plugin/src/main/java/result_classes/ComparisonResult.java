package result_classes;

public class ComparisonResult {
    public double meanGroupA;
    public double meanGroupB;
    public double pValue;
    public boolean significant;
    public long samplesA;
    public long samplesB;

    public ComparisonResult() {}

    public ComparisonResult(double meanGroupA, double meanGroupB, double pValue, boolean significant,
                            long samplesA, long samplesB) {
        this.meanGroupA = meanGroupA;
        this.meanGroupB = meanGroupB;
        this.pValue = pValue;
        this.significant = significant;
        this.samplesA = samplesA;
        this.samplesB = samplesB;
    }
}
