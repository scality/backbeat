// Kept in the user metadata so it survives across processes: the transition
// processor bumps it on failure, and clears it once the object reached its new
// location.
const TRANSITION_ATTEMPT_MD = 'x-amz-meta-scal-s3-transition-attempt';

/**
 * Read the transition attempt count of an object.
 * @param {ObjectMD} objMD - object metadata
 * @return {Number|undefined} attempt count, or undefined if the object was
 * never transitioned
 */
function getTransitionAttempt(objMD) {
    const attempt = Number.parseInt(objMD.getValue()[TRANSITION_ATTEMPT_MD], 10);
    return Number.isInteger(attempt) ? attempt : undefined;
}

module.exports = {
    TRANSITION_ATTEMPT_MD,
    getTransitionAttempt,
};
