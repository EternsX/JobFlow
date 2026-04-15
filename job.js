class Job {
    constructor(id, description, maxRetries = 3, priority = 0) {
        this.id = id;
        this.description = description;
        this.priority = priority;
        this.tries = 0;
        this.maxRetries = maxRetries;
        this.status = 'pending';
        this.result = null;
        this.errors = [];
    }

    // simulate async job task (replace with real logic)
    async perform() {
        console.log(`Performing job: ${this.description}`);
        // simulate random failure
        if (Math.random() < 0.8) {
            throw new Error('Random failure occurred');
        }
        return `Job ${this.id} done`;
    }
}

export default Job;