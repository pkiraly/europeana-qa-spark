package com.nsdr.spark.uniqueness;

/**
 *
 * @author Péter Király <peter.kiraly at gwdg.de>
 */
public class TfIdf {

	private String term;
	private double tf;
	private double df;
	private double tfIdf;

	public TfIdf(String term, double tf, double df, double tfIdf) {
		this.term = term;
		this.tf = tf;
		this.df = df;
		this.tfIdf = tfIdf;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public double getTf() {
		return tf;
	}

	public void setTf(double tf) {
		this.tf = tf;
	}

	public double getDf() {
		return df;
	}

	public void setDf(double df) {
		this.df = df;
	}

	public double getTfIdf() {
		return tfIdf;
	}

	public void setTfIdf(double tfIdf) {
		this.tfIdf = tfIdf;
	}

	@Override
	public String toString() {
		return term + "(tf=" + tf + ", df=" + df + ", tfIdf=" + tfIdf + ')';
	}
}
