class Job {
  constructor({ id, description, priority = 0, tries = 0, maxTries = 3 }) {
    this.id = id;
    this.description = description;
    this.priority = priority;
    this.tries = tries;
    this.maxTries = maxTries;
  }

  static from(raw) {
    const job = new Job({
      id: raw.id,
      description: raw.description,
      priority: Number(raw.priority ?? 0),
      tries: Number(raw.tries ?? 0),
      maxTries: Number(raw.maxTries ?? 3),
    });

    job.validate();
    return job;
  }

  validate() {
    if (!this.id) throw new Error('Job must have an id');
    if (!this.description) throw new Error('Job must have a description');
    if (this.maxTries <= 0) throw new Error('maxTries must be > 0');
  }

  canRetry() {
    return this.tries < this.maxTries;
  }

  incrementTries() {
    this.tries += 1;
  }

  getBackoffDelay() {
    return 1000 * Math.pow(2, this.tries - 1);
  }

  async execute() {
    try {
      await this.perform();
      return { ok: true };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  perform() {
    if (Math.random() < 0.7) {
      throw new Error('Failure');
    }
  }

  toDTO() {
    return {
      id: this.id,
      description: this.description,
      priority: this.priority,
      tries: this.tries,
      maxTries: this.maxTries,
    };
  }
}

export default Job;