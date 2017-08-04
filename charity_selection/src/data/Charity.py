class Charity:
    """
    For every registered charity a number of attributes is available. We record only:
    Income: Charity accounts are prepared on the basis of guidelines called the Charities Statement
    of Recommended Practice (SoRP). The financial information displayed on the Register of Charities
    follows the categories defined in the SoRP. The charity’s income is shown as the total incoming
    resource recorded in the charity’s Statement of Financial Activities (SoFA) which forms part of
    their accounts.
    out of which Voluntary: Gifts, donations and legacies from the public and grants from Government
    and other charitable foundations which provide core funding or are of a general nature.
    Distinctive characteristic: the donor receives nothing in return.
    Spending: Charity accounts are prepared on the basis of guidelines called the Charities
    Statement of Recommended Practice (SoRP). The financial information displayed on the Register of
    Charities follows the categories defined in the SoRP. The charity’s spending is the total
    resource expended as shown in the charity’s Statement of Financial Activities (SoFA) which forms
    part of their accounts.
    out of which on Charitable Activities: Costs incurred by the charity in supplying goods or
    services to meet the needs of its beneficiaries.
    """

    def __init__(self, name):
        self.f_name = name
        self.id = None
        self.Income = None
        self.Voluntary = None
        self.Spending = None
        self.Charitable_Acts = None


# End of file
