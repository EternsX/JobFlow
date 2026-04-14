import './AddJob.css'

export default function AddJob() {
    return (
        <div className="add-job">
            <h1>Add Job</h1>
            <div className="form">
                <div className="input-group">
                    <label htmlFor="job">Job</label>
                    <input type="text" id="job" name="job" />
                </div>
                <div className="input-group">
                    <label htmlFor="description">Description</label>
                    <input type="text" id="description" name="description" />
                </div>
            </div>
        </div>
    )
}